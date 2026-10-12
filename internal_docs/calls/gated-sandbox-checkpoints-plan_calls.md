# Judgement Calls: Gated Sandboxes by Default, with Memory Checkpoints

Decisions made while writing
[`plans/gated-sandbox-checkpoints-plan.md`](../plans/gated-sandbox-checkpoints-plan.md),
and while executing it (Phases 0 to 5 on a host without KVM). **Entries are ranked by project impact, highest
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

## 2. The flip to gated-by-default is built but held closed until Phase 0 passes

**Call.** Phase 2 ships everything but the flip itself. `auto` resolving to
`gated` for sandboxes on microVM nodes is implemented and tested, and held
behind `SANDBOX_GATED_BY_DEFAULT = false` in `src/structs/network.rs` until
G0.1 and G2.1 pass on a KVM host (Phase 0, the plan's gate). A store opens it
per instance for tests (`Store::set_sandbox_gated_default`), and a manager
started with `LEGION_SANDBOX_GATED_DEFAULT=1` opens it for verification runs.
That variable is not a supported setting, and the public docs describe the
default as `host` until the flip.

**Alternatives.** (a) Flip it now, since every piece is unit-tested. (b) Hold
all of Phase 2 (default deny, the static agent, the refusals) until Phase 0.
(c) A manager config field to opt in.

**Trade-off.** The headline of the plan, safe by writing nothing, is not on
yet: a sandbox that declares no network still gets loopback only. The code
path that resolves `auto` to `gated` has never placed a real guest.

**Why (simplicity, correctness).** The work ran on a host with no KVM, so no
gated guest has booted. Flipping the default would make every networked
sandbox depend on a vsock path that has only run against Unix sockets. (b)
holds back pieces that do not depend on the VMM at all. (c) adds a setting
that exists only until Phase 0 and that AGENTS.md says not to add. One
`const` and one test-only switch keep the flip a one-line change once G0.1
passes.

## 3. A gated allocation with no policy denies all egress, on every runtime

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

## 4. Default deny ships for every gated runtime before the sandbox flip

**Call.** `gated-default` denial (entry 3) is on as of Phase 2 for every
gated allocation: an explicit `network.mode = "gated"` on a `container`,
`exec`, or `sandbox` task, and a `require` policy that imposes `gated`, with
no task block or policy object covering it, now denies instead of allowing
with attribution. Only the sandbox `auto` flip waits on Phase 0 (entry 2).

```text
HTTP/1.1 403 Forbidden
Legion-Denied-By: gated-default
```

**Alternatives.** (a) Hold default deny together with the flip. (b) Leave
`container` and `exec` on allow-all, since worker-plane-auth's Phase 3 makes
Linux containers gated by default.

**Trade-off.** A breaking change for anyone who set `gated` and relied on
allow-all-with-attribution; they add `egress = { policy = "allow" }`. The
other agent's Phase 3 (containers gated by default) lands on top of this
behavior rather than choosing its own.

**Why (simplicity).** The verdict does not depend on a VMM, so it needs no
Phase 0 evidence, and one rule for every gated runtime is what the plan
decided. Shipping it here, once, keeps the two plans from each writing a
default-deny path.

## 5. A restore retargets the vsock socket to its own allocation through VMM overrides, not a stable path

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

**Trade-off.** Gated memory checkpoints need Firecracker 1.16.1 (entry 18),
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

## 6. Legion addresses TAP guests from a per-bridge pool and re-addresses them on every restore

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

## 7. Fixed TAP names and pinned MACs are removed

**Call.** `config.network.tap` and `config.network.mac` go. Every TAP is
per instance (`lgf` plus a hash of the instance id) on `bridge`, with a MAC
derived from the allocation id. Submit refuses the old fields:

```text
config.network.tap is gone: Legion creates a TAP per instance on network.bridge and leases
the guest an address from the bridge's pool
Hint: Set bridge = "<the bridge tap0 was on>", and declare sandbox.bridges.<bridge> in the
worker config (docs/task-runtime-sandbox.md, "Addressing")
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

## 8. A TAP sandbox's bridge constraint is written into the stored task at submit

**Call.** Workers advertise their declared bridges as one list attribute,
`sandbox.bridges = ["br0"]`, and the sandbox runtime's submit normalization
adds `{ key = "sandbox.bridges", operator = "contains", value = "br0" }` to a
TAP task's `constraints`. An object-form constraint block becomes the
equivalent list. `job get` shows the added rule, and resubmits don't stack
copies.

**Alternatives.** (a) Inject the rule in the scheduler at candidate
construction, leaving the stored task untouched. (b) One boolean attribute
per bridge (`sandbox.bridge.br0 = true`) with an `=` rule that object form
can express.

**Trade-off.** The stored job differs from what the user wrote, and an
object-form constraint block comes back as a list. A node that stops
declaring a bridge strands its TAP sandboxes as pending rather than failing
them at start.

**Why (simplicity, performance).** Normalization already rewrites sandbox
tasks at submit (guest size), so the rule rides an existing path and
placement runs the ordinary constraint check with no scheduler change and
no benchmark risk. (a) would make the scheduler know about one runtime's
config. (b) puts one attribute per bridge in every node record and a
variable key in every task. A visible rule is also how an operator finds out
why a TAP task is pending.

## 9. A guest's lease is a file in its allocation dir, and the node's leases are a scan

**Call.** `lease()` writes `guest-addr.json` into the allocation's scratch
dir. The set of leases on a node is the set of those files under the worker's
allocation root, read under a process-wide lock on every lease. Destroy
removes the scratch dir, which is the release. A worker restart rebuilds the
set from disk with nothing of its own to recover.

**Alternatives.** (a) A table in the worker's state store. (b) An in-memory
allocator rebuilt from allocation dirs at worker start.

**Trade-off.** Each lease scans every allocation dir on the node (one small
file read per sandbox). A scratch dir left behind by a crash holds its
address until the worker's cleanup removes the dir.

**Why (simplicity).** The lease's lifetime is the allocation dir's lifetime,
so storing it there makes release and rebuild correct by construction, with
no second record to keep in sync. Leasing happens at a sandbox start or
restore, never on a hot path, and a node runs at most a few hundred
sandboxes.

## 10. The guest agent ships as a static binary

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

## 11. Every sandbox port the node can reach is fronted, TAP included, and only a loopback-only sandbox's are refused

**Call.** Phase 5 fronts every port a sandbox advertises when the datapath
can reach its guest, with or without an ingress chain: a gated guest over
host-initiated vsock (`CONNECT <port>`), a TAP guest at its leased bridge
address. Submit refuses `network.ports` only on a sandbox nothing can reach.
This supersedes entries 30 and 31, the Phase 2 refusal.

```text
task "workspace" declares network.ports, but nothing can reach a port in its guest: with no
config.network and network.mode not "gated", the guest has loopback only
Hint: set network.mode = "gated" to serve the ports through the node datapath, or wire the guest
to a bridge (config.network = { mode = "tap", bridge = "br0" })
```

**Alternatives.** (a) Phase 5 as planned, gated only, with TAP sandboxes'
ports left as they were: exported into the guest, reachable on the bridge,
never at the address the catalog lists. (b) Ship gated first and leave TAP
as a TODO. (c) Map TAP ports with DNAT rules on the node. (d) Keep accepting
ports on a loopback-only sandbox, as before.

**Trade-off.** A TAP guest's ports now go through a user-space splice on the
node, and its workload must bind its bridge address or `0.0.0.0`. A sandbox
spec with ports and no network config, accepted before, fails at submit.

**Why (simplicity, ease).** A wake-on-request gateway splices to the
`address:host_port` the catalog lists, and should not care how a guest is
wired. With a per-connection dial target (`TaskDial`), TAP is one more arm
beside vsock, reading a lease file already on disk, so it cost a few dozen
lines rather than a phase. (c) adds per-guest iptables state that must
follow restores and forks, which Legion avoids everywhere else. (d) is the
§6.8 footgun: ports in the catalog that accept nothing.

## 12. A gated sandbox's upstreams ride the guest agent to the node's own upstream listener, on a vsock socket

**Call.** For each `connect.upstreams` entry of a gated sandbox, the guest
agent listens on `127.0.0.1:<local port>` (and the VIP, entry 16) and
relays every connection to host vsock port `<local port>`. The worker binds
that upstream's ordinary listener state on
`<alloc dir>/.sandbox/vsock.sock_<local port>` (a `UnixListener` arm of
`start_listener`) instead of a loopback port, so balancing, HTTP mode, the
egress chain, the upstream's handlers, and fail-closed while the policy is
pending are the same code. `capture_ready` counts these sockets with the
proxy and the resolver, and `net doctor` names an unbound one:

```text
upstream db is not bound on the guest's vsock socket /var/lib/legion/allocs/7/.sandbox/vsock.sock_20003
```

**Alternatives.** (a) Leave upstreams unreachable in a sandbox and document
it. (b) Send upstream traffic through the egress proxy (the agent's 1080):
no per-upstream listener in the guest, but a client that is not
proxy-aware (`psql`, a gRPC channel) cannot use it, and the proxy would have
to map a destination back to an upstream to apply its HTTP block and
handlers. (c) One vsock port for every upstream, with a header from the
agent naming which. (d) A sandbox-specific forwarder on the host.

**Trade-off.** One vsock host socket per upstream per sandbox, and the
upstream's local port doubles as a vsock port, so 1080 and 53 are refused as
`localPort` (entry 38). The forwarder's connection path became generic
over the stream type, and the PROXY header of a `proxy` hop carries no
peer address for a vsock connection.

**Why (simplicity, extensibility).** The upstream listener is where every
upstream feature already lives, so binding the same state on another socket
makes each future upstream feature work in a sandbox for free. (b) splits
upstream behavior across two code paths and breaks non-HTTP clients. (c)
adds a framing the agent must speak and the host must trust, for a socket
count that is small anyway. One number naming the guest port, the vsock
port, and `LEGION_UPSTREAM_PORT_<KEY>` keeps `net doctor`, the agent's log,
and the env in agreement, as entry 15 does for ingress.

## 13. A restored sandbox keeps the ports it was captured with, and the datapath dials those

**Call.** The boot record (`checkpoint-boot.json`) records `guest_ports`,
port label to the port the workload binds, and the checkpoint's runtime
state carries it. A restore records each label's captured port when the
checkpoint has one, else the new allocation's bind port, and writes the
record before the guest resumes. The ingress target reads it per connection
(`TaskTarget::port`). The identity disk sets `LEGION_PORT_<label>` in
`resume.env` to the same number.

**Alternatives.** (a) Re-request the captured ports at placement. (b) Have
the guest agent listen on the new port and relay to the old. (c) Dial the
new allocation's port.

**Trade-off.** The new allocation's own `LEGION_PORT_<label>` can differ
from what its guest listens on; the record is the truth. A small file read
per inbound connection.

**Why (correctness, simplicity).** A memory restore brings back a process
that already called `bind`. (c) dials a port nobody listens on, on every
restore whose placement moved the port, which is exactly when a
wake-on-request gateway needs it to work. (a) couples placement to a
checkpoint and fails when the port is taken. (b) needs the agent to learn
both numbers and does nothing for a TAP guest, which has no agent in the
path. Reading per connection also covers the listener being bound before
the restore writes the record.

## 14. A restore retargets the guest agent's upstream relays, not the node's listener

**Call.** The checkpoint records `guest_upstreams`, upstream key to the
loopback port the workload dials, like `guest_ports`. A restore keeps those
ports in the guest (the processes dialing them ride the memory), and the
identity disk carries each upstream's new allocation local port
(`GuestUpstream::vsock_port`). The agent keeps one target per upstream,
keyed by its loopback port, that every listener of the upstream reads per
connection, and the restore stores the new port there. The node always
binds `vsock.sock_<the allocation's own local port>` and never reads the
boot record for upstreams.

**Alternatives.** (a) The node binds the captured port, read from the boot
record when it builds listener specs (built first, then dropped). (b)
Re-request the captured local ports at placement. (c) Dial the new
allocation's port in the guest (impossible: the workload holds the old
address in memory).

**Trade-off.** The agent holds a little mutable state per upstream, the
identity disk carries a second number per upstream, and `--upstream` grew
a `PORT@VSOCK` form that a boot never uses. An upstream the job removed
while the instance was suspended keeps its listener in the guest, relaying
to a socket nobody binds, so its dials are refused.

**Why (correctness, performance).** The node's listener must exist before
the guest dials, and the boot record is written during the restore, after
the heartbeat that delivered it bound the allocation's listeners. Under (a)
upstreams were broken from the resume until the next heartbeat applied the
record, up to one heartbeat interval, which the manager stretches past a
minute on a large fleet, and every heartbeat read a file per sandbox
upstream. Retargeting in the guest
works from the first connection and costs one atomic store. Ingress makes
the opposite choice (entry 13) because there the node dials, so it can
read the record per connection.

## 15. The vsock port is the port the workload binds, and a sandbox gets no twin of its own

**Call.** `CONNECT <port>` names the port the workload binds
(`AllocPort::bind_port`, what `LEGION_PORT_<label>` says), and the guest
agent listens on that vsock port and relays to `127.0.0.1:<same port>`. A
sandbox gets a private twin exactly when any task does (an ingress chain).
Without one, the guest binds the advertised number itself.

**Alternatives.** (a) Always give sandboxes a twin. (b) A fixed vsock port
per label (`10000 + index`), mapped to the workload port by the agent. (c)
One vsock port for all ingress, with a port header from the host.

**Trade-off.** Guest vsock ports and the agent's egress host ports (1080,
53) are different spaces, which a reader of the agent must keep straight. A
gated guest's `127.0.0.1:1080` is the agent's, so a workload cannot serve on
1080 there (documented).

**Why (simplicity).** One number names both ends, so `net doctor`, the
agent's log, and the workload's env agree. The guest's network stack is its
own, so the advertised number cannot collide with the datapath's listener on
the node, and a twin would only spend a port from the node's range. (b) and
(c) add a mapping the guest must learn and a restore must keep consistent.

## 16. The guest agent serves each upstream's VIP, so `<key>.svc.legion` works in a gated guest

**Call.** The gated resolver (vsock 53) already answers `<key>.svc.legion`
with the allocation's VIPs. The agent also listens on `<vip>:<canonical
port>` in the guest, relaying to the same vsock port as the loopback
listener, and the node binds no VIP for a sandbox. A restore adds the new
allocation's VIPs and keeps the old ones relaying, retargeted with the rest.

**Alternatives.** (a) Document that names do not work in a gated guest and
point at `LEGION_UPSTREAM_<KEY>`. (b) Answer `<key>.svc.legion` with
`127.0.0.1` and rely on SRV records for the port.

**Trade-off.** The agent binds addresses in 127.42/16 inside the guest,
which needs the guest's `lo` to own 127/8 (Linux does, with no
configuration). Old VIPs linger in a restored guest until it reboots.

**Why (ease).** `db.svc.legion:5432` working the same in a container and a
sandbox is the point of name-is-config: an agent's code should not branch
on where it runs. (b) breaks every client that does not read SRV records,
which is nearly all of them. Lingering old VIPs are what a resolver cache
holding the old answer needs.

## 17. The guest code is its own workspace crate, `legion-guest`

**Call.** The guest agent, resume handler, identity-disk format, env file
format, and netlink re-addressing move into `guest/` (crate `legion-guest`, a
library and the static binary). The worker's `legion guest-agent` and `legion
guest-resume` subcommands pass their raw arguments to the crate's parser, so
both binaries take the same command line.

**Alternatives.** (a) Share source files with `#[path]`. (b) A second binary
target in the main crate (a musl build would then compile the whole
worker's dependency tree). (c) Keep the code in the main crate and build the
worker static.

**Trade-off.** A Cargo workspace and a second crate to version and publish
(it must be published before `legion-rs`).

**Why (simplicity, ease).** The guest needs a small static artifact, and a
crate with four dependencies builds one in seconds for any target. One
parser and one identity-disk module serve both sides, so the worker and the
guest cannot disagree about the format.

## 18. Gated memory checkpoints need Firecracker 1.16.1, checked at capture

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

## 19. A live capture of a gated Firecracker sandbox resets its open connections, and we say so

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

## 20. Ingress takes no Firecracker 1.17 floor until Phase 0 measures #6100

**Call.** Host-initiated vsock runs on whatever Firecracker a gated sandbox
runs on. The plan's version table names 1.17.0 for host-initiated RX no
longer sticking after a bare pause and resume (#6100), for Phase 0 to
measure. No floor is enforced; the public doc names the release and says
to run it for gated sandboxes that serve ports.

**Alternatives.** (a) Refuse ports on gated sandboxes on nodes below 1.17.
(b) Refuse a live memory snapshot or fork (the bare pause and resume) of a
gated sandbox with ports below 1.17.

**Trade-off.** On Firecracker 1.16, a gated origin that serves ports may
stop receiving inbound connections after a live capture, if #6100 reaches
this path, until it is restarted.

**Why (simplicity).** No guest has booted on this host, so the bug's reach
is unmeasured, and a floor is a version gate with its own refusal surface on
three APIs. G5.1 measures it, and the floor check from entry 18 is where a
1.17 rule goes if it does.

## 21. Declared `connect.upstreams` are not subject to `gated-default`

**Call.** A gated task's declared upstreams (`connect.upstreams`) are an
explicit allowance in the task block, so their listeners dial as before.
`gated-default` denies only egress that nothing covers: the proxy's
destinations and resolver names.

**Alternatives.** Deny upstream dials too unless an egress rule also allows
the service.

**Trade-off.** An upstream is reachable from a gated task without an
`egress.allow` entry for it.

**Why (simplicity).** Declaring an upstream is already the user naming what
the task may reach. Requiring a second, matching egress rule would make
every gated service consumer write the same thing twice.

## 22. A gated resolver forwards while the allocation's policy is pending

**Call.** A gated allocation's resolver answers `NXDOMAIN` for names nothing
allows (`gated-default`) only once its policy is known. While the policy is
still pending (the first heartbeat after placement), queries are forwarded.
The egress proxy fails closed during the same window, so nothing connects.

**Alternatives.** Fail DNS closed while pending too.

**Trade-off.** In the seconds before the policy arrives, a guest can resolve
any name, which is a narrow DNS exfiltration channel.

**Why (ease).** Images that resolve at boot (package managers, agents
probing their API) would cache a failure from the pending window and keep it
after the policy arrives. Resolution grants no connection, and the window is
one heartbeat.

## 23. `connections` counts open egress connections too, and `network` stamps bytes as they move

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

## 24. `connections` stays refused on a sandbox that is not gated, though TAP ports are fronted now

**Call.** A TAP sandbox's inbound connections cross the datapath since entry
11, but submit still refuses `connections` on any sandbox that is not
gated. The error says "outbound traffic" now.

**Alternatives.** Accept `connections` on a TAP sandbox that declares ports,
counting its ingress only.

**Trade-off.** A TAP sandbox has no signal for an inbound request that
sends nothing for a while. `network` sees the bytes its guest sends.

**Why (ease).** On a sandbox the signal exists to keep a harness awake
through a quiet model call (entry 23), an outbound connection a TAP guest
makes on its own NIC. Counting only the inbound half would accept a spec
whose author believes the call is covered.

## 25. Upstreams on TAP and loopback-only sandboxes stay unreachable, and are not refused

**Call.** Only a gated sandbox reaches its upstreams. A TAP-wired or
loopback-only sandbox that declares `connect.upstreams` is accepted and
placed. Its `LEGION_UPSTREAM_<KEY>` names `127.0.0.1:<port>` in the guest,
where nothing listens, so dials are refused. The public docs say so, and
TODO.md tracks the TAP case.

**Alternatives.** (a) Refuse `connect.upstreams` at submit on a sandbox that
will not run gated, as ports on a loopback-only sandbox are (entry 11). (b)
Serve a TAP guest's upstreams on the node's bridge address, attributed by
the guest's leased source address.

**Trade-off.** A spec that cannot work is accepted, and the failure shows as
a refused connection in the guest rather than a submit error.

**Why (scope).** (b) is its own design: a listener per bridge address,
attribution by a source address the guest controls (unlike a vsock socket),
and the lease moving on restore. It was deferred so this change stays on the
gated path. (a) is the better end state for the loopback-only case, but
while `auto` resolves to `host`, it would refuse every default sandbox that
declares upstreams, which needs its own look at what that does to existing
specs.

## 26. An absent `config.network` means gated, and an explicit `none` means an air gap

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

## 27. A gated-or-TAP shape mismatch is refused at restore, not at the API

**Call.** `instance create --from-snapshot` no longer compares the task's
network shape with the snapshot's at the API. The restore refuses by name
(`checkpoint ... has a vsock datapath and no NIC, this task ...`), and
`suspend.onRestoreFailure` decides what follows.

**Alternatives.** Refuse at the API by comparing the task spec with the
snapshot's `runtime_state`.

**Trade-off.** The create succeeds, and the failure arrives as a
`snapshot.restore_failed` event and a cold boot (or a failure with `"fail"`).

**Why (simplicity).** The restore is the only place that knows the shape
it is restoring into, since a `require` policy can change a task's mode
after the snapshot. One rule in one place, as the plan's §6.2 states.

## 28. The agent closes dead relays with a shutdown, so a client sees a close

**Call.** After a restore the guest agent shuts both directions of every
open relay (`shutdown(Both)` on the guest and vsock sockets). A client
inside the guest sees EOF or a closed connection promptly.

**Alternatives.** Close with `SO_LINGER 0`, which sends a reset, as the plan
worded it.

**Trade-off.** Clients see a clean close rather than `ECONNRESET`. A client
that treats EOF as a complete response could mistake a cut-off body for a
whole one, though HTTP clients check `Content-Length`.

**Why (simplicity).** The relay threads own their sockets, so closing them
from the restore watcher would race those threads. A shutdown is safe from
any thread and still never hangs, which is the guarantee the docs make.

## 29. Every gated sandbox gets a resolver, with or without upstreams

**Call.** `dns_specs` emits a resolver for every gated sandbox. Today it
needs a VIP block, which only allocations with upstreams get.

**Alternatives.** Assign a VIP block to every gated allocation.

**Trade-off.** None worth naming for sandboxes. Gated `container` and `exec`
tasks without upstreams keep the same gap, recorded as a TODO.

**Why (ease).** A default sandbox has no upstreams, so without this its
guest has no DNS at all. The sandbox resolver binds a Unix socket and never
uses the VIP address, so a block would spend one of 256 per node for
nothing.

## 30. Ingress into gated sandboxes is its own phase, and refused until then

> **Superseded by entry 11.** Phase 5 serves a gated sandbox's ports and
> lifted this refusal. Kept as the record of the interim.

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

## 31. Ingress ports are refused on any sandbox that will run gated

> **Superseded by entry 11.** Phase 5 serves a gated sandbox's ports and
> lifted this refusal. Kept as the record of the interim.

**Call.** Submit refuses `network.ports` on a sandbox that runs gated: an
explicit `gated`, or `auto` once the default is open. A sandbox with a
`config.network` (TAP) is exempt, since it resolves to `host`.

```text
task workspace declares network.ports, but ingress into a gated sandbox is not supported yet
Hint: wire the guest to a bridge (config.network = { mode = "tap", bridge = "br0" }) to serve ports
```

**Alternatives.** Refuse only an explicit `gated`.

**Trade-off.** Once the default is open, a ports-declaring sandbox with no
network config must add a TAP block to submit.

**Why (ease).** An `auto` sandbox with ports would otherwise place gated and
accept connections that never reach the guest. The refusal points at the
working shape until Phase 5.

## 32. A checkpoint on a hand-named TAP moves onto the instance's own device

**Call.** A resume whose checkpoint names a TAP device other than the
instance's own (one taken on `tap = "tap0"` before Phase 4) moves the NIC
onto the instance's own `lgf` device, the same move a fork makes, rather than
refusing the restore.

**Alternatives.** Refuse the restore (`the task now names TAP lgf...`) and
let `onRestoreFailure` cold-boot it.

**Trade-off.** On Firecracker the move needs 1.12, so an old checkpoint on an
older Firecracker fails at load instead of with a named refusal.

**Why (ease).** Removing `tap` would otherwise strand every existing TAP
checkpoint, and the plan promises they restore and get a Legion address.
The move is a path forks already exercise.

## 33. The guest address rides the network status report as two fields

**Call.** The worker reports a TAP guest's lease on `NetAllocStatus`
(`guest_address`, `guest_bridge`), and the manager copies it onto
`InstanceInfo` for `instance get` and `instance list`. A TAP sandbox is
reported even when nothing else about it is on the datapath.

**Alternatives.** (a) The usage report (`AllocationUsage`). (b) One string,
`10.88.128.7 (br0)`.

**Trade-off.** The address is in memory on the manager, from the node's last
heartbeat, so it is unset for a few seconds after a start and after a
manager restart.

**Why (ease).** Usage reports are sent only with resource enforcement on,
and the address must not depend on that. Two fields let an agent use the
address without parsing, and the CLI prints the combined form.

## 34. A restore waits briefly for the datapath's socket, then resumes fail-closed

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

## 35. The guest agent owns the resume watch

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

## 36. The agent learns its ingress ports from its command line at boot and from the identity disk at restore, and never unbinds

**Call.** The generated init passes `--ingress-port <port>` per advertised
port to `legion guest-agent`. A restore writes
`ResumeIdentity::ingress_ports`, and the agent's resume watch listens on any
port it does not have yet. Listeners are never closed.

**Alternatives.** (a) Read the ports from the identity disk at boot too.
(b) Reconcile on restore: close listeners for ports the identity no longer
names.

**Trade-off.** A restore that drops a port leaves a guest listener that
nothing dials.

**Why (simplicity).** The init line already carries the agent's flags, and
the root disk's fingerprint covers it, so a changed port list rebuilds the
disk. The identity disk is what a restore rewrites and what the agent
already watches. The listeners are guest memory and survive a restore, and
the restored workload keeps its captured ports (entry 13), which the
datapath keeps dialing, so (b) would close exactly the ports in use.

## 37. Every gated guest uses CID 3

**Call.** Delete `vsock_cid` and give every gated guest CID 3.

**Alternatives.** Keep the per-allocation CID.

**Trade-off.** If a VMM registers CIDs with the host kernel, two guests
collide. Phase 0 checks this on both VMMs.

**Why (simplicity).** Both VMMs implement vsock over Unix sockets in user
space, the guest agent dials the host (CID 2), and Firecracker cannot change
the CID on load. A per-allocation CID is stale after every restore and
shared by every fork anyway, so it only looks meaningful.

## 38. An upstream `localPort` of 1080 or 53, and a guest port of 1080, are refused on a gated sandbox

**Call.** Submit refuses, on a sandbox that will run gated, an advertised
port the guest would bind on 1080 (`static` or `to`), and an upstream
`localPort` of 1080 or 53:

```text
task "workspace" connect.upstreams entry "db" sets localPort 53, but a gated sandbox's upstreams are
relayed to the node on vsock ports of the same number, and 1080 and 53 are the guest's egress proxy
and resolver
Hint: pick another localPort, or drop it to take a dynamic one
```

**Alternatives.** (a) Map upstream vsock ports into a range of their own
(vsock ports are 32 bits, so `65536 + local port` never collides), which
would leave only the guest's `127.0.0.1:1080` reserved. (b) Let it fail at
start.

**Trade-off.** Two port numbers a container may use are unavailable to a
gated sandbox's upstreams.

**Why (simplicity).** Dynamic ports come from far above both, so only a
pinned port can hit this, and saying so at submit costs nothing. (a) breaks
the one-number rule (entry 12) for every upstream to spare two pinned
values almost nobody picks.

## 39. Widen the proxy variables, defer transparent capture

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

## 40. Capture side effects come back as `notes` on the reply

**Call.** `CreateSnapshotReply` and `ForkInstanceReply` gain `repeated
string notes`. A live capture of a gated Firecracker sandbox adds one, and
the CLI prints it as `note: ...`.

**Alternatives.** Append the note to the human message, or have the CLI work
it out from the instance.

**Trade-off.** Two proto fields for one note today.

**Why (extensibility).** An agent reads the JSON, not the message, and only
the manager knows the node's VMM. A list leaves room for the next side
effect without another field.

## 41. The worker finds the guest binary beside itself, or where an override says

**Call.** The worker copies `legion-guest` from its own directory, or from
`LEGION_GUEST_BINARY` when set. A set override that does not exist fails the
sandbox's start rather than falling back. With neither, the worker copies
itself and warns once at start. The root disk's fingerprint includes the
binary's path, size, and mtime.

**Alternatives.** A worker config field. Silent fallback when the override
is wrong.

**Trade-off.** One more environment variable on the worker.

**Why (ease).** Beside the worker is how a release ships it, with no
setting. A wrong override is a mistake to name, not hide. Fingerprinting the
binary also fixes a stale agent surviving a worker upgrade in a reused root
disk.

## 42. Removed fields stay in the parser only to refuse them by name

**Call.** `SandboxNetworkConfig` keeps `tap` and `mac` as hidden fields
(skipped by the schema) whose only use is the refusal with the replacement,
and the sandbox runtime runs its typed parse before the schema check, so the
named error wins over "unknown field".

**Alternatives.** Let `deny_unknown_fields` reject them generically.

**Trade-off.** Two dead fields in a struct, and the typed parse's error is
the first one a malformed config gets.

**Why (ease).** This is not a compatibility shim: the fields do nothing but
refuse. A user or agent moving a spec forward gets the exact fix (`Set bridge
= "<the bridge tap0 was on>"`) instead of a schema error.

## 43. Only a resume asks for its previous address

**Call.** A restore asks the pool for the address its checkpoint recorded
only when it is a resume of the same instance. A fork, and a create from a
snapshot, take the lowest free address.

**Alternatives.** Every restore prefers the recorded address.

**Trade-off.** A fork of a destroyed origin never inherits the origin's
address even when it is free.

**Why (correctness).** A fork answering on its origin's address is the bug
Phase 4 exists to fix, and preferring it would bring it back whenever the
origin is on another node or briefly stopped.

## 44. The lease goes first on the kernel command line, and resolv.conf is baked in

**Call.** `ip=` comes before the task's `kernel_args`, so a task's own `ip=`
wins at boot (the kernel takes the last one), and the resume handler still
applies the lease after a restore. The root disk build writes
`/etc/resolv.conf` from the pool's `dns`, replacing a symlink, and the
resolvers are part of the disk's fingerprint.

**Alternatives.** Append `ip=` last, so Legion always wins at boot. Write
resolv.conf from init at every boot.

**Trade-off.** A task can override its boot address, which Legion then
reports wrongly until the first restore.

**Why (ease).** `kernel_args` is the documented escape hatch for the kernel
command line, and an operator debugging a guest should be able to use it.
Baking resolv.conf in needs no init step and no tool in the image.

## 45. The worker re-checks the Firecracker floor before a gated capture

**Call.** Besides the manager's refusal at suspend, snapshot, and fork, the
worker refuses a gated memory capture on Firecracker older than 1.16.1
before pausing the guest.

**Alternatives.** Check on the manager only.

**Trade-off.** The rule lives in two places (one shared function).

**Why (correctness).** A node-local pressure suspend never asks the manager,
and a capture the node cannot restore would lose the guest's network on
resume.

## 46. The Firecracker floor check resolves the task's own VMM first

**Call.** The 1.16.1 check reads the task's explicit `hypervisor` first, then
the node's default VMM, and compares that VMM's own version attribute
(`sandbox.vmm.firecracker.version`).

**Alternatives.** Use the node's default VMM only, as the TAP fork check
does.

**Trade-off.** None worth naming.

**Why (correctness).** A node with both VMMs installed would otherwise judge
a Firecracker task by cloud-hypervisor's version, or the reverse.

## 47. `net policy show` resolves `auto`, and the policy replies carry hints

**Call.** `ShowNetPolicyReply.effective_mode` now resolves `auto` the way
placement would (it used to echo `auto`), with `notes` explaining it, and
`TestNetPolicyReply` gains `hint` with the one-line fix for a
`gated-default` denial.

**Alternatives.** Leave `auto` unresolved, and put the fix in `reason`.

**Trade-off.** Two proto fields.

**Why (ease).** An agent asking "will this be gated" gets the answer, and a
denial carries its fix in a field it can act on.

## 48. `egress-unenforced` appears only once the gated default is open

**Call.** The `egress-unenforced` instance detail (a sandbox whose `auto`
would be gated, running on a dev backend) is reported only while the default
is open.

**Alternatives.** Report it now.

**Trade-off.** None until the flip.

**Why (simplicity).** Before the flip, `auto` means `host` everywhere, so
nothing is unenforced relative to what the user would get on Linux.

## 49. A restore waits on the existing net status, with no new marker

**Call.** The restore's wait for the datapath (entry 34)
polls for the egress proxy's socket, and an unbound socket is reported
through the existing `net_status` problems, which now name the socket path.

**Alternatives.** A new "datapath ready" marker file the worker writes.

**Trade-off.** None worth naming.

**Why (simplicity).** The socket's existence is the readiness signal, and
`net doctor` already shows net status problems.

## 50. The Phase 0 boot test binds the real datapath in-process

**Call.** `sandbox_gated_boots_*` start the node's real egress proxy and
resolver for one allocation inside the test, then boot the guest through the
sandbox runtime, rather than starting a manager and worker. They default to
`example.com` (allowed) and `example.org` (denied), so they need outbound
HTTP from the host.

**Alternatives.** A full cluster test. A local origin server on the host's
loopback.

**Trade-off.** The test does not cover the heartbeat path that assigns the
policy, and it needs the internet.

**Why (simplicity).** The question Phase 0 answers is whether the VMM's vsock
path carries the datapath, and the in-process datapath is the same code a
worker runs. A loopback origin would be refused once the worker-plane-auth
host guard lands.

## 51. HTTP-mode ingress dials through the task target, so a hand-off proxy is dialed from the host

**Call.** HTTP mode's per-request dial to the task goes through the same
`TaskTarget::connect` as the L4 splice: loopback, a namespace, a guest's
vsock, or a bridge address. A `proxy` hand-off or a mesh endpoint is a TCP
dial from the host namespace. Before, an HTTP-mode hand-off on a gated task
was dialed inside the task's namespace, where nothing routes to it.

**Alternatives.** Keep `HttpSettings.netns` and add a vsock case beside it.

**Trade-off.** None beyond where a hand-off dial originates, which is the
fix.

**Why (simplicity, extensibility).** One way to reach a task, used by both
modes, so the next way (another VMM transport) is one more `TaskDial` arm.

## 52. Anything but `OK <n>` within two seconds is a failed dial

**Call.** The host side of the handshake writes `CONNECT <port>\n` and reads
one line, at most 64 bytes, byte by byte so nothing past the newline is
consumed. `OK <n>` (any `n`, a trailing `\r` tolerated) succeeds. A close
names the port (`nothing in the guest listens on vsock port <port>`), and an
error line, a long line, or two seconds of silence fail the dial too. The
connection then closes like any failed ingress dial.

**Alternatives.** (a) Parse the VMMs' error lines and map them to distinct
errors. (b) A longer budget for a guest still booting.

**Trade-off.** A guest that takes over two seconds to accept looks like a
dead one to that client.

**Why (simplicity).** Firecracker and cloud-hypervisor both close the stream
when nothing listens and neither documents an error line, so the only
contract is the `OK` line. A byte-at-a-time read of one short line is cheap,
and reading past it would swallow the guest's first bytes. Two seconds is
the datapath's dial budget everywhere else.
