# Judgement Calls: Gated Sandboxes by Default, with Memory Checkpoints

Decisions made while writing
[`plans/gated-sandbox-checkpoints-plan.md`](../plans/gated-sandbox-checkpoints-plan.md),
before any code exists. **Entries are ranked by project impact, highest
first. The number is the rank.** Implementation-time calls get inserted at
their rank as the phases execute. If a citation elsewhere names an entry by
number, fix it when you renumber.

## 1. `auto` resolves to `gated` for sandboxes on microVM nodes, and to `host` on dev backends

**Call.** A `sandbox` task with no `network.mode` and no `config.network`
resolves to `gated` at placement when the node's `sandbox.vmm` is Firecracker
or cloud-hypervisor and `hypervisor` is not `dev`. On a dev backend (Apple
`container`, Linux `dev`) it resolves to `host`, and the instance says so:

```text
detail: egress-unenforced: dev mode runs sandbox workspace without Legion's egress policy
(Apple container has no vsock to the datapath)
```

An explicit `network.mode = "gated"` is never downgraded and still fails at
start on a dev backend.

**Alternatives.** (a) Keep `auto` as `host` and tell agent platforms to set
`gated`. (b) Resolve `auto` to `gated` everywhere and refuse dev backends,
as an explicit `gated` does. (c) Make `gated` the literal default value
instead of a resolution of `auto`.

**Trade-off.** The same spec is enforced on a Linux node and unenforced on a
Mac, and the difference shows up as a detail and a warning, not an error.
Placement reads one more node attribute for every sandbox allocation.

**Why (simplicity, ease).** The user's goal is that the safe network is what
you get by writing nothing. (a) keeps the default that the agent sandboxes
guide has to talk people out of. (b) breaks the macOS dev loop for every
sandbox spec, which is where authors first run them. (c) would refuse the
same spec on a Mac. Resolving `auto` keeps one spec working everywhere and
keeps the hard guarantee in the explicit form, which a `require` policy can
impose. The resolution already happens at placement, which has the node.

## 2. A gated allocation with no policy denies all egress, on every runtime

**Call.** When no task block and no policy object covers a gated
allocation's egress, every connection is denied with the new source
`gated-default`, on `sandbox`, `container`, and `exec` alike. Today the same
allocation allows everything with attribution. Opening it is one line:

```nickel
network = { egress = { allow = ["pypi.org", "files.pythonhosted.org"] } },
# or, today's behavior:
network = { egress = { policy = "allow", accessLog = true } },
```

A shipped, opt-in `configs/policies/agent-sandbox-egress.ncl` covers the
common registries.

**Alternatives.** (a) A curated allowlist on by default (package registries,
GitHub, model APIs), as Claude Code on the web's Trusted level and Codex's
"Common dependencies" preset do. (b) Allow all with attribution, as gated
does today and as E2B and Modal do by default. (c) Deny by default on
sandboxes only, and keep allow-all for gated `container` and `exec`.

**Trade-off.** Every new sandbox spec hits a `403` on its first `pip
install` until its author writes an allowlist. Gated `container` and `exec`
tasks that relied on allow-all must add `policy = "allow"`.

**Why (simplicity, security).** Behind `CONNECT` the proxy sees a host and a
port, so a default that opens `registry.npmjs.org` or `github.com` opens
`npm publish` and `git push` too. A shipped list goes stale against where
tooling connects (users report exactly that against hosted Trusted lists),
and no default list knows a platform's model provider. The previous sandbox
default was loopback only, so default deny loses no reachability. Making it
uniform across runtimes keeps "gated" a one-sentence concept. (b) gives
agent-written code the internet by default. (c) is two defaults for one
mode.

## 3. A restore retargets the vsock socket to its own allocation through VMM overrides, not a stable path

**Call.** The vsock host socket stays `<alloc>/.sandbox/vsock.sock`, derived
from the allocation id. Every restore of a gated checkpoint points the VMM
at the restoring allocation's path: Firecracker with `vsock_override` on
`PUT /snapshot/load` (1.16 and later), cloud-hypervisor through the
rewritten `config.json` that fork TAPs already use. If Phase 0 finds either
override broken, both VMMs switch to a per-VMM mount namespace that bind
mounts the allocation's dir on a constant path.

**Alternatives.** (a) Put the socket in the device-link directory
(suspend calls entry 55's suggestion). (b) A stable per-instance directory,
keyed by the instance id like `own_tap_name`, so a resume needs no override.
(c) The mount namespace as the primary design.

**Trade-off.** Gated memory checkpoints need Firecracker 1.16.1 (entry 7),
and the design depends on two VMM features Legion has not run yet.

**Why (correctness, simplicity).** (a) is a security bug: a VMM opens a disk
once at load, but resolves `<uds_path>_<port>` on every guest connect, and
the link directory is shared by every fork of a lineage. After a fork
repoints it, the origin's traffic would reach the fork's listener and be
judged as the fork. (b) still needs an override for forks and
create-from-snapshot, adds instance-keyed paths to the datapath and a
directory lifecycle, and ties cross-node restores to identical worker state
paths. The override covers every restore kind with the path the datapath
already binds before launch. (c) works on every VMM version and needs no
API, and is kept as the fallback, but it adds mount plumbing to every VMM
spawn to solve what two documented API fields solve.

## 4. Legion addresses TAP guests from a per-bridge pool and re-addresses them on every restore

**Call.** The worker config declares a pool per bridge
(`sandbox.bridges.br0 = { subnet, gateway, range, dns }`). A TAP guest gets a
leased address on the kernel command line at boot (`ip=…`), and the guest
agent applies a fresh lease through netlink after every restore and fork.
`instance get` shows it. A bridge without a pool cannot host TAP sandboxes.

**Alternatives.** (a) On restore, flush `eth0` and re-run the image's DHCP
client. (b) Firecracker's clone recipe: a network namespace and NAT per
clone, every clone with the same guest IP. (c) Keep the docs' advice (use
DHCP, renew in `onResume`).

**Trade-off.** Operators configure an address range per bridge and keep it
out of any DHCP pool on that bridge. A guest's address changes across
nodes, and across a same-node resume when the range is exhausted. Legion now
owns IPAM, a small new subsystem.

**Why (ease, correctness).** The user asked that addressing stop being the
user's job. (a) depends on a DHCP client in the image (Debian slim has none),
a DHCP server on the bridge, and timing, and when any fails, forks collide
silently, which is the bug. (b) needs no guest change but takes the guest
off the operator's bridge, and L2 adjacency there is the reason to pick TAP
over gated. (c) is the footgun. A lease always applies, works on any image,
and gives Legion an address it can show.

```nickel
config = { network = { mode = "tap", bridge = "br0" } },   # address comes from br0's pool
```

## 5. Fixed TAP names and pinned MACs are removed

**Call.** `config.network.tap` and `config.network.mac` go. Every TAP is
per instance (`lgf` plus a hash of the instance id) on `bridge`, with a MAC
derived from the allocation id. Submit refuses the old fields:

```text
Error: config.network.tap is gone: Legion creates a TAP per instance.
Hint: set bridge = "<the bridge tap0 was on>" and drop tap
```

Disk forks of TAP sandboxes become legal, since each boots on its own TAP.

**Alternatives.** (a) Refuse a fixed TAP only on keyed tasks. (b) Keep fixed
TAPs as an expert option with the current one-per-node refusals.

**Trade-off.** An operator who pre-wired a special TAP (custom routing,
tc filters) must move that wiring to the bridge or to a bridge of its own.

**Why (simplicity).** A fixed name fits one instance per node, collides on
every fork and second instance, and carries three runtime refusals (suspend
calls 85, 86, 87) that exist only to name the collision. (a) keeps the
refusals for slot tasks. AGENTS.md says to remove rather than shim, and one
TAP shape is easier to explain than two.

## 6. The guest agent ships as a static binary

**Call.** Phase 2 builds `legion-guest`, a statically linked musl binary
(the guest agent and resume handler), installed beside the worker and copied
into the guest. The worker falls back to copying itself, with a warning,
when it is missing.

**Alternatives.** (a) Keep copying the glibc worker binary and document the
musl gap, as today. (b) Build the whole worker static. (c) A
separately versioned agent artifact pulled at launch.

**Trade-off.** A second build artifact per release and per architecture.

**Why (ease).** A gated default makes the agent mandatory for every
networked sandbox, and the docs' own example image, `alpine:latest`, cannot
run a glibc binary, so (a) makes the default fail closed on the most common
small image. (b) touches every dependency of the worker for a guest-side
need. (c) adds a fetch to the boot path and a version matrix.

## 7. Gated memory checkpoints need Firecracker 1.16.1, checked at capture

**Call.** A memory suspend, snapshot, or fork of a gated sandbox on
Firecracker older than 1.16.1 is refused before capture, with the version
and the fix. Because memory-checkpoint compat is an exact VMM version match
(suspend calls entry 70), the check at capture covers every later restore.

```text
Error: instance user-42 is gated, and node w-03 runs Firecracker 1.15.0, which cannot move a
restored guest's vsock to a new socket (that needs Firecracker 1.16.1 or later)
Hint: upgrade Firecracker on the node, or use --checkpoint disk
```

**Alternatives.** (a) 1.16.0, the release that added `vsock_override`. (b)
A placement filter keeping gated checkpointable sandboxes off old
Firecracker nodes.

**Trade-off.** Nodes on older Firecracker run gated sandboxes with disk
checkpoints only.

**Why (correctness, simplicity).** 1.16.1 fixes guest-to-host connections
timing out after restore when a TX descriptor was in flight (#5958), which
is the gated datapath's only traffic pattern. (b) is placement machinery for
a version skew that an upgrade fixes, and the refusal names the upgrade.

## 8. A live capture of a gated Firecracker sandbox resets its open connections, and we say so

**Call.** Accept that Firecracker closes every open guest vsock connection
at `SnapshotCreate`, so a live `snapshot create --memory` or a fork drops
the running origin's open connections. Document it, print a one-line note on
those commands for gated Firecracker sources, and tell harness authors to
retry interrupted calls.

**Alternatives.** (a) Refuse live memory snapshots and forks of gated
sandboxes on Firecracker. (b) Stop the guest agent's relays before capture
and replay buffered bytes after.

**Trade-off.** An agent mid-stream on a model call loses that stream when
someone snapshots or forks it.

**Why (ease).** It is VMM behavior, not Legion's, and it matches what
happens on a suspend anyway. (a) removes fork, the feature agent platforms
want most, to avoid a retry. (b) cannot work: the reset is in the guest
kernel's transport, below the agent.

## 9. `connections` counts open egress connections too, and `network` stamps bytes as they move

**Call.** The egress proxy wraps every splice in the activity ledger's
`TrackedStream`. `network` sees bytes during a long stream, and
`connections` counts open ingress and egress connections. `connections`
stays out of the default signal list.

**Alternatives.** (a) A separate `egress-connections` signal. (b) Keep
`connections` ingress-only and wait for the `hint` signal.

**Trade-off.** An author who adds `connections` keeps a sandbox awake through
any idle keepalive it holds, not only a model call.

**Why (simplicity).** The job is "keep my harness awake while it waits on a
model", and on a gated sandbox that wait is an outbound connection the proxy
already holds. (a) is a second knob for a one-word difference. (b) leaves the
gap the suspend plan's calls entry 53 recorded. The hot-path cost is the one
the ingress side already pays.

## 10. An absent `config.network` means gated, and an explicit `none` means an air gap

**Call.** `config.network = { mode = "none" }` stays and now means no vsock,
no agent, and no DNS. Leaving `config.network` out means gated.

**Alternatives.** (a) A task-level `network.mode = "none"`. (b) Drop `none`
since gated with default deny reaches nothing either.

**Trade-off.** Absent and explicit `none` behave differently, which a reader
must learn from the schema table.

**Why (simplicity).** (a) adds a fourth `network.mode` value that means
nothing on `container` and `exec`. (b) loses the case gated cannot serve: an
image the agent cannot run on, and a compute sandbox that should have no
door at all, not a door that is shut by policy.

## 11. Every gated sandbox gets a resolver, with or without upstreams

**Call.** `dns_specs` emits a resolver for every gated sandbox. Today it
needs a VIP block, which only allocations with upstreams get.

**Alternatives.** Assign a VIP block to every gated allocation.

**Trade-off.** None worth naming for sandboxes. Gated `container` and `exec`
tasks without upstreams keep the same gap, recorded as a TODO.

**Why (ease).** A default sandbox has no upstreams, so without this its
guest has no DNS at all. The sandbox resolver binds a Unix socket and never
uses the VIP address, so a block would spend one of 256 per node for
nothing.

## 12. Ingress into gated sandboxes is its own phase, and refused until then

**Call.** Phase 2 refuses advertised ingress ports on a gated sandbox at
submit. Phase 5 adds ingress over host-initiated vsock (`CONNECT <port>`)
and lifts the refusal.

**Alternatives.** Build ingress into Phase 2, before making gated the
default.

**Trade-off.** Between Phases 2 and 5, a platform that needs to reach its
sandbox uses a TAP.

**Why (simplicity).** Today the combination is accepted and fails at
runtime, because the ingress listener dials into a network namespace a
sandbox does not have. Refusing it is the honest interim. Ingress needs
agent-side vsock listeners and a Firecracker version floor Phase 0 measures,
and the default flip should not wait on it.

## 13. A restore waits briefly for the datapath's socket, then resumes fail-closed

**Call.** Before resuming a restored gated guest, wait up to 5 seconds for
`vsock.sock_1080`. If it is still missing, resume anyway and report
`net_capture_unavailable`.

**Alternatives.** Fail the restore and let `onRestoreFailure` decide.

**Trade-off.** A guest can come back with no working door for a while, and
its first calls fail.

**Why (ease).** The listener is normally bound in the same heartbeat tick
before the launch, so a miss is a transient bind error. Failing the restore
would turn that into a cold boot that loses the user's running state, while
a missing socket only means each dial is reset, which is fail-closed.

## 14. The guest agent owns the resume watch

**Call.** `legion guest-agent` absorbs `legion guest-resume`'s identity-disk
watch when both are needed, so on a restore it can drop its own relays in
process.

**Alternatives.** Keep two processes and have the resume handler signal the
agent.

**Trade-off.** One process now does two jobs, and a crash in one takes the
other.

**Why (simplicity).** The relay drop is what makes an old cloud-hypervisor's
half-open connections fail fast instead of hanging. A signal between two
processes needs a pid file and a race-free handshake for no gain.

## 15. Every gated guest uses CID 3

**Call.** Delete `vsock_cid` and give every gated guest CID 3.

**Alternatives.** Keep the per-allocation CID.

**Trade-off.** If a VMM registers CIDs with the host kernel, two guests
collide. Phase 0 checks this on both VMMs.

**Why (simplicity).** Both VMMs implement vsock over Unix sockets in user
space, the guest agent dials the host (CID 2), and Firecracker cannot change
the CID on load. A per-allocation CID is stale after every restore and
shared by every fork anyway, so it only looks meaningful.

## 16. Widen the proxy variables, defer transparent capture

**Call.** Phase 2 sets lower-case proxy variables and `NODE_USE_ENV_PROXY=1`
for every gated runtime. Transparent redirect inside the guest stays an open
question.

**Alternatives.** Build in-guest transparent capture before the default
flip.

**Trade-off.** Tools that ignore proxy variables (Node before 22.21, SSH
without a proxy command, anything dialing raw IPs) still fail in a default
sandbox, and the failure is a connection error, not a readable `403`.

**Why (simplicity).** The variables cover curl, apt, pip, npm, Go, and
modern Node for the cost of a few env entries. Transparent capture needs
netfilter in the guest kernel and SNI parsing in the agent, and it should be
sized by the denials Phase 2 actually records.
