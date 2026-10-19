# Plan: Gated Sandboxes by Default, with Memory Checkpoints

> **Judgement calls:** [`calls/gated-sandbox-checkpoints-plan_calls.md`](../calls/gated-sandbox-checkpoints-plan_calls.md)

Status: **IN EXECUTION** (written 2026-10-11, Phases 1 to 5 built
2026-10-12 on a Linux host without KVM).

- **Phase 0 (the gate): pending.** No gated guest has booted. The opt-in
  boot tests (`sandbox_gated_boots_firecracker`, `_cloud_hypervisor`) and
  checks G0.1 to G4.1, with exact commands, are in the verification doc's
  [Gated sandboxes](../suspend-resume-linux-verification.md#gated-sandboxes)
  section.
- **Phase 1: built.** Gated sandboxes take memory checkpoints (suspend,
  snapshots, forks, publish), with the vsock retargeted on every restore and
  the Firecracker 1.16.1 floor. Unit- and integration-tested against fake
  VMMs; G1.1 to G1.5 pending.
- **Phase 2: built, flip held.** Default deny (`gated-default`) on every
  gated runtime, the resolver, the static `legion-guest` agent, and the
  submit refusals ship. The flip of `auto` to `gated` for sandboxes is
  implemented and tested but held closed behind `SANDBOX_GATED_BY_DEFAULT =
  false` until G0.1 and G2.1 pass (calls entry 2).
- **Phase 3: built.** `connections` counts gated egress, and `network`
  stamps bytes mid-stream. G3.1 pending.
- **Phase 4: built.** `sandbox.bridges` pools, leases, `ip=` at boot,
  netlink re-addressing on restore (run for real in a network namespace),
  the implicit bridge constraint, and `config.network.tap` and `mac`
  removed. G4.1 pending.
- **Phase 5 (ingress): built.** The ingress listener dials a gated guest
  through the VMM's base vsock socket (`CONNECT <port>`), the guest agent
  listens on a vsock port per advertised port and relays to loopback, and
  the submit refusal is lifted. Deviations, each a calls entry: TAP
  sandboxes' ports are fronted in the same change (dialed at the leased
  guest address), the guest's ports ride the checkpoint so a restore dials
  the ports the restored workload holds, and only a loopback-only
  sandbox's ports are still refused. Unit-tested against a fake VMM and a
  fake guest; G5.1 pending.
- **Added after Phase 5: upstreams in gated sandboxes, built.** The guest
  agent listens on each `connect.upstreams` entry's loopback port and VIP
  and relays to host vsock port `<local port>`, where the worker binds the
  upstream's ordinary listener; a restore retargets the agent's relays to
  the new allocation's local ports. TAP and loopback-only sandboxes still
  cannot reach their upstreams (calls entries 12, 14, 16, 25, and 38).

Deliberately deferred and named:
transparent capture inside the guest, UDP and ICMP through the datapath,
`exec` over the guest agent's vsock, cooperative egress policy in macOS dev
mode, and the `legion netd` process split. Each is in §4 (non-goals) or
§11 (open questions).

**Relationship to existing plans.** This plan lifts the limitation recorded
in [suspend plan calls 55 and 48](../calls/suspend-resume-and-oversubscription-plan_calls.md#55-gated-sandboxes-are-not-memory-checkpointable-yet)
and executes the "`sandbox` moves to `gated` when its vsock transport lands"
note in `resolve_network_mode` (`src/structs/network.rs:39`). It reuses the
restore machinery of
[`suspend-resume-and-oversubscription-plan.md`](suspend-resume-and-oversubscription-plan.md)
(device links, the identity disk, the resume handler, TAP retargeting for
forks) and the gated datapath of
[`service-mesh-plan.md`](service-mesh-plan.md) (M9, sandbox vsock). It
reshapes TAP networking from that plan's calls 43, 85, 86, and 87.

---

## The plan in one page

An agent sandbox wants two things Legion offers today, but never together:

- **Per-sandbox egress policy.** Gated networking gives a microVM no NIC at
  all. Its only door is a vsock to the node datapath, where policy,
  handlers, and identity judge every connection.
- **Memory checkpoints.** Suspend, resume, snapshots, and fork keep the
  running processes, the shell, and the warm caches.

Today the combination is refused at submit, because the guest's vsock socket
path is baked into the VMM's saved state. So the agent sandboxes guide tells
platforms to pick: an auto TAP on an operator bridge (checkpoints, no Legion
policy) or gated with disk checkpoints (policy, cold boots).

This plan removes the choice:

- **Restores retarget the vsock socket** to the restoring allocation's own
  path, the way forks already retarget their TAP device. Firecracker 1.16
  added `vsock_override` on snapshot load. cloud-hypervisor reads the path
  from the checkpoint's `config.json`, which Legion already rewrites for
  forks. The datapath keeps deriving the path from the allocation id, so it
  binds the listeners before the restore, and the restored guest's traffic
  is judged under the new allocation's current policy.
- **Gated becomes the default network for `sandbox` tasks**, and a gated
  allocation with no policy denies all egress. A sandbox opens exactly the
  hosts its job or a policy object allows.
- **The TAP fallback stops being a footgun.** Legion addresses TAP guests
  from a per-bridge pool and re-addresses them on every restore and fork, so
  forks never share an IP. Fixed TAP names and pinned MACs go away.
- **Idle detection sees what a gated sandbox does**: live bytes and open
  connections through the egress proxy, so a harness waiting on a model call
  stays awake when the platform asks it to.

The phases: prove gated on a real VMM (0), make it checkpointable (1), make
it the default (2), sharpen idle detection (3), fix TAP addressing (4), and
let traffic into a gated guest (5). Each ships on its own, in that order,
except Phase 4, which is independent.

---

## 1. Problem statement

**What is impossible today.** A `sandbox` task with `network.mode = "gated"`
and memory checkpoints fails job validation
(`src/structs/snapshot.rs:463`):

```text
Error: task "workspace" asks for memory checkpoints, but sandbox memory checkpoints don't support gated networking yet
Hint: set snapshots.memory = false for disk-only snapshots, or drop network gating for this task
```

When a stored policy imposes `gated`, the sandbox launches without checkpoint
support (`src/task_plugins/sandbox.rs:1140`), and `SuspendInstance`,
`CreateSnapshot --memory`, and `ForkInstance` fail with `FailedPrecondition`
(`src/manager/server/suspend_rpcs.rs:113`, `snapshot_rpcs.rs:210`,
`sandbox_checkpoint.rs:407`).

**What a platform does instead.** It runs agent sandboxes on
`config.network = { mode = "tap", tap = "auto", bridge = "br0" }` and puts
egress filtering on the bridge, outside Legion. That loses per-sandbox
policy, attribution, `403` denials an agent can read, and the service graph.
Or it runs gated with `suspend.checkpoint = "disk"`, and every resume is a
cold boot that loses the agent's running state.

**The TAP path has its own footgun.** A memory fork copies whatever address
the guest configured. Three forks of `user-42` on `br0` answer on
`user-42`'s IP, and the docs tell the user to run DHCP in the guest and renew
the lease in `onResume` (`docs/suspend-and-resume.md`, "Networking for
forks", and the "Forks copy the guest's addresses" limitation). That is
Legion's job, not the user's.

**And the default is wrong for the use case.** A `sandbox` with no network
config boots with loopback only. `network.mode = "auto"` resolves to `host`
for every runtime (`src/structs/network.rs:42`), so the gated datapath an
agent sandbox most needs is opt-in, and its unit-tested code has never booted
on a real VMM.

## 2. Prior art

**Firecracker.** The vsock device is host-side Unix sockets. A guest connect
to host port P reaches `<uds_path>_P`, and the host dials into the guest
through `uds_path` with a `CONNECT P` line
([vsock.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/vsock.md)).
On snapshot, open vsock connections are closed: the device sends
`VIRTIO_VSOCK_EVENT_TRANSPORT_RESET` during `SnapshotCreate`, and guest
listen sockets survive
([snapshot-support.md, "Vsock device reset"](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)).
The [changelog](https://github.com/firecracker-microvm/firecracker/blob/main/CHANGELOG.md)
has the pieces this plan depends on:

| Version | Change | Why it matters here |
| --- | --- | --- |
| 1.10.0 | TRANSPORT_RESET is now delivered after restore (#4796) | Before it, a restored guest waits forever on a reset connection |
| 1.12.0 | `network_overrides` on `PUT /snapshot/load` (#4731) | What fork TAPs already use |
| 1.15.0 | vsock local port reuse fixed across restore (#5688) | Snapshots must be regenerated |
| 1.16.0 | `vsock_override: { uds_path }` on `PUT /snapshot/load` (#5323), and an RX race after restore fixed (#5882) | The retarget this plan uses |
| 1.16.1 | Guest-to-host connections no longer time out after restore when a TX descriptor was in flight (#5958) | Exactly our traffic pattern |
| 1.17.0 | RX no longer stuck after a bare pause and resume (#6100) | Host-initiated connections, Phase 5 |

Firecracker's own answer to cloned networking,
[network-for-clones.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/network-for-clones.md),
runs each clone in its own network namespace with the same TAP name and the
same guest IP, and NATs each clone to a unique address. We take its point
that a restored guest's identity must be fixed by the host, and reject the
mechanism for the TAP fallback (§6.7, calls entry 6).

**cloud-hypervisor.** Same `<socket>_<port>` convention, ported from
Firecracker ([vsock.md](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/vsock.md)).
Restore takes `source_url`, `resume`, `memory_restore_mode`, and `net_fds`.
The documented way to change a restored VM's devices is to edit the
snapshot's `config.json`
([snapshot_restore.md](https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/snapshot_restore.md)),
which Legion already does for fork TAPs. v52.0 resets vsock connections on
restore "to avoid stale half-open connections on the guest side" (#7958,
[release notes](https://newreleases.io/project/github/cloud-hypervisor/cloud-hypervisor/release/v52.0)).
Older releases may leave half-open guest connections, which §6.3 handles in
the guest agent.

**Hosted agent sandboxes and their egress defaults.**

- [E2B](https://e2b.dev/docs/network/internet-access): internet on by
  default, `allowInternetAccess: false` or `network.allowOut` / `denyOut`
  to restrict.
- [Modal](https://modal.com/docs/guide/sandbox-networking): open by default,
  `block_network` or a CIDR allowlist.
- [Codex cloud](https://developers.openai.com/codex/cloud/agent-internet):
  internet off during the agent phase by default. When on, a domain
  allowlist ("None" or a "Common dependencies" preset) and an allowed-methods
  setting.
- [Claude Code on the web](https://code.claude.com/docs/en/claude-code-on-the-web):
  a "Trusted" allowlist by default, with None, Full, and Custom levels. The
  local [sandboxed Bash](https://workos.com/blog/agent-sandbox-egress-defaults)
  pre-allows no domains.

The products that run agent-written code by default deny or allowlist. The
products that are general compute open everything. Legion's sandbox is the
former, and a curated allowlist has a known failure mode: a list that drifts
from where tooling actually connects, which users report against Claude Code
on the web's Trusted list. §6.5 and calls entry 3 decide.

**Kubernetes and Consul.** Kubernetes `NetworkPolicy` allows everything
until a policy selects a pod. Default-deny is a pattern you add. Consul's
service mesh with `default_policy = "deny"` denies until an intention
allows, and its transparent proxy redirects traffic with iptables inside the
pod's namespace so unmodified tools work. We take Consul's default and
reject transparent redirect for now (§4, calls entry 39). Inside a microVM
it would be safe, since the vsock stays the only door, and it is the
leading follow-on.

**Kata Containers** talks to its in-guest agent over vsock too, and treats
the agent as untrusted plumbing. Legion's guest agent already decides
nothing.

## 3. Jobs to be done, by persona

**Agent platform builder (job author)**
- When I give each user a sandbox, I want it to reach only what my agent
  needs, so code the agent writes cannot ship my users' data anywhere else.
- When a user walks away, I want their sandbox to vanish to disk with its
  processes intact and come back in under a second, so I don't pay for idle
  VMs and the user doesn't wait for a cold boot.
- When I fork a sandbox to try three approaches at once, I want each fork to
  be its own machine on the network, so their traffic, logs, and denials
  never get mixed up.
- When I write a spec, I want the safe networking to be what I get by
  writing nothing, so I can't forget it.

**Security / platform admin**
- When a sandbox is resumed hours later, I want it to run under today's
  rules, so a policy I tightened while it slept still applies.
- When agents keep hitting a blocked host, I want to see which sandbox tried
  and which rule said no, so I can decide whether to open it.

**Cluster operator**
- When I set up sandbox nodes, I want a short list of what to install and
  configure, so a node either works or says what is missing.
- When a team needs raw networking (a bridge to a lab network, a protocol
  the proxy can't carry), I want a supported escape hatch, so they don't
  invent one.

**On-call responder**
- When a resumed sandbox can't reach the internet, I want one command that
  says whether the policy, the datapath, or the guest is at fault, so I can
  fix the right thing.

**Autonomous agent (inside the sandbox)**
- When a request is refused, I want to read why from the response, so I
  stop retrying and report the blocked host instead.

*Not in scope:* an agent driving Legion's API gets no new surface here. The
existing `instance`, `snapshot`, and `net` commands and MCP tools cover it.

### User scenarios

1. **The default sandbox.** A platform submits a sandbox spec with a
   `suspend` block and no networking. Each instance boots gated. The agent's
   `pip install` fails with a `403` naming the `gated-default` rule. The
   platform adds `network.egress.allow` for its registries and model API,
   resubmits, and the next request goes through. Three minutes after the
   user leaves, the sandbox suspends with memory, and it resumes on the next
   request with its shell intact.
2. **The policy changes while it sleeps.** The security admin applies a
   `require` policy denying `api.github.com` while `user-42` is suspended.
   On resume, the restored guest's first request to GitHub is denied and
   attributed to `user-42`'s new allocation.
3. **Three forks.** The platform forks `user-42` three times. Each fork's
   egress lands in the access log and the service graph under its own
   instance. On a TAP sandbox, each fork comes up with its own address on
   `br0`, with no `onResume` script.

## 4. Goals and non-goals

**Goals**

- Memory suspend, resume, snapshot, restore, fork, and publish work on gated
  sandboxes, on both VMMs, same node and cross-node.
- A restored or forked gated guest's traffic is judged under the new
  allocation's identity and current effective policy. A policy changed while
  the instance was suspended applies at resume.
- Forks of a gated sandbox cannot collide on the network. There is no guest
  address to collide on.
- `sandbox` tasks are gated by default, with default-deny egress.
- `none` (loopback only) and TAP stay as explicit choices. TAP is the
  documented escape hatch for raw networking.
- TAP guests are addressed by Legion, at boot and after every restore and
  fork. No doc tells a user to run DHCP in the guest.
- Idle detection counts live egress bytes and, when asked, open connections
  on gated sandboxes.
- Every doc that states the limitation is corrected in the phase that lifts
  it.

**Non-goals**

- **Transparent capture.** Tools that ignore proxy variables still fail in a
  gated sandbox. Phase 2 widens the variables Legion sets, and §11 tracks the
  in-guest redirect as the follow-on.
- **UDP beyond DNS, ICMP, and raw sockets** through the datapath. The proxy
  speaks TCP (HTTP `CONNECT`, absolute-URI HTTP, SOCKS5 `CONNECT`), and the
  guest agent relays only TCP 1080 and UDP 53
  (`src/worker/netd/guest_agent.rs`, `proxy.rs:832`). The service mesh plan's Phase 7 owns
  UDP.
- **Exec into a microVM** over the guest agent's vsock. The device and agent
  are now always present, so this gets cheaper, but it is its own plan.
- **Gated in macOS dev mode.** Apple `container` has no vsock to the node
  datapath. Dev mode stays unenforced and says so (§6.6).
- **Zero-drop connections across a suspend.** A memory checkpoint closes
  every open guest connection on both VMMs. We document it and do not try to
  migrate TCP state.
- **A per-VMM mount namespace or jailer.** It is the fallback if Phase 0
  finds the VMM overrides unusable (§6.2), not the plan of record.

## 5. Background: what exists today

**The gated sandbox datapath** (service mesh M9).

- Launch: a gated sandbox gets `VmSpec.vsock = VsockSpec { cid:
  vsock_cid(allocation.id), socket: <alloc>/.sandbox/vsock.sock }`
  (`src/task_plugins/sandbox.rs:1128`, `microvm.rs:367-380`). The CID is
  `3 + allocation_id % (u32::MAX - 3)`. cloud-hypervisor gets
  `--vsock cid=…,socket=…` (`microvm.rs:487`), Firecracker a `"vsock"` config
  block (`microvm.rs:546`). A TAP in config is refused at start
  (`sandbox.rs:1037`), and a dev backend is refused at start
  (`sandbox.rs:1713`).
- Guest: the generated init starts `/legion/legion guest-agent` before the
  command (`microvm.rs:679`), writes `nameserver 127.0.0.1`
  (`microvm.rs:765`), and copies the worker's own binary in
  (`install_guest_agent`, `microvm.rs:786`). The agent relays
  `127.0.0.1:1080` TCP and `127.0.0.1:53` UDP to host CID 2, ports 1080 and
  53, **dialing a fresh vsock stream per guest connection and per DNS query**
  (`guest_agent.rs:200-292`). It holds no long-lived host connection.
- Host: on every successful heartbeat, `apply_upstreams` binds the egress
  proxy on `<alloc>/.sandbox/vsock.sock_1080` and the resolver on `_53`,
  derived from the allocation id alone (`src/worker/daemon.rs:2369`,
  `2185`, `2603`), **before** reconcile launches the task in the same tick
  (`daemon.rs:1508-1550`). Both listeners create the parent dir and unlink a
  stale socket first (`proxy.rs:191`, `dns.rs:174`).
- Policy: each listener is keyed by allocation id. The proxy evaluates
  `context.policy_for(allocation_id)` on every connection (`proxy.rs:307`),
  denies with `policy-pending` until the allocation's policy arrives, and
  checks the edge-boot gate first. **With no policy at all, a gated
  allocation allows everything** (`egress_connect_verdict`,
  `src/worker/netd/policy.rs:194`), as `docs/network-handlers.md` says.
- Env: a gated allocation gets `LEGION_EGRESS_PROXY`, `HTTP_PROXY`,
  `HTTPS_PROXY`, `ALL_PROXY`, and `NO_PROXY`, upper case only
  (`src/structs/network.rs:649`).
- Status: `net_status` reports a gated sandbox's capture as ready when the
  proxy is bound. The resolver is checked only when a VIP block exists
  (`daemon.rs:2475-2510`).

**Gaps found in the recon** (each becomes a task below):

- A VIP block, and so the resolver, is assigned only to allocations with
  upstreams (`src/storage/store.rs:12160`). A gated sandbox with no
  `connect.upstreams` has no `_53` listener, so DNS in the guest fails. The
  same holds for gated `container` and `exec` tasks.
- The ingress listener dials the private port inside the allocation's
  network namespace (`src/worker/netd/ingress.rs:522`). A gated sandbox has
  no namespace, so ingress into it cannot work, and nothing refuses the
  combination.
- The proxy records bytes only when a splice ends
  (`copy_bidirectional` then `record_bytes`, `proxy.rs:788`). An idle sweep
  sees a long model stream at its start and its end, never during.
- `connections` counts open ingress connections only
  (`src/worker/netd/activity.rs`). Combined with the ingress gap, it can
  never fire on a gated sandbox, though submit accepts it there
  (`src/structs/suspend.rs:688`).
- The guest agent is the worker binary, which is dynamically linked against
  glibc. On a musl image such as `alpine:latest`, it does not run and the
  guest has no network (fails closed, `docs/network-handlers.md`).

**Memory checkpoints of sandboxes** (suspend plan Phases 3b to 6).

- A checkpoint launch points the VMM at symlinks under
  `<allocs>/.vmdev/<link_key>/`, with a random `link_key` per boot
  (`sandbox_checkpoint.rs:322-367`). Every restore of that lineage,
  including concurrent forks, reuses the key and repoints the links under
  `LINK_LOCK` before the VMM opens them (`sandbox_checkpoint.rs:1094-1170`).
  This is safe for disks because **a VMM opens a disk once, at load**.
- TAPs: the boot record keeps the attached TAP (`SandboxCheckpointState.tap`,
  `record_attached_tap`). A resume recreates it by name
  (`ensure_restore_tap`). A fork moves the NIC to its own `lgf…` device,
  through `network_overrides` on Firecracker (version-gated at 1.12 by
  `firecracker_can_override_tap`, `src/structs/fork.rs:72`, checked in
  `fork_rpcs.rs:133`) and a rewritten copy of `config.json` on
  cloud-hypervisor (`retarget_cloud_hypervisor_tap`,
  `sandbox_checkpoint.rs:979`).
- The resume handler (`legion guest-resume`, `src/task_plugins/guest_resume.rs`)
  polls the identity disk and, on a restore, reseeds the RNG, steps the
  clock, sets the hostname and the NIC's MAC (`ip link set eth0 address`),
  writes identity files, and runs `onResume`. It runs only when the sandbox
  was launched for checkpoints or has file tokens, and **gated sandboxes are
  never launched for checkpoints** (`sandbox.rs:1061`).
- Memory-checkpoint compat is an exact match on VMM name and version, CPU,
  and guest shape (suspend calls entry 70), so a restore always runs on the
  VMM version that captured it.
- Guest network addressing is not Legion's today. The init brings up
  loopback only (`microvm.rs` init), and a TAP guest must configure `eth0`
  itself.

## 6. Proposed design

### 6.1 The surface, before and after

The default sandbox, Phases 1 and 2 shipped:

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "sandbox",
    stateful = {
      mode = "keyed",
      idSource = "caller",
      storage = { path = "/workspace", mode = "block", params = { size = "10G" } },
    },
    config = {
      image = "ghcr.io/acme/agent-base:2026-10",
      command = ["/usr/local/bin/harness"],
    },
    # No network block: the guest boots gated, with no NIC, and denies all
    # egress. Open what the agent needs:
    network = {
      egress = {
        allow = [
          "api.anthropic.com",
          "pypi.org", "files.pythonhosted.org",
          "registry.npmjs.org",
          "github.com", "codeload.github.com",
        ],
      },
    },
    resources = { cpu = 250, memory = 512, memoryMax = 4096 },
    suspend = {
      checkpoint = "memory",
      idle = { after = "3m", signals = ["network", "cpu", "connections"] },
    },
    snapshots = { ttl = "14d", memory = true },
  }],
}
```

What changed for the author: nothing to declare for gated, `connections` is
meaningful, and memory checkpoints are accepted.

The network choices for a sandbox, after this plan:

| Spec | Guest gets | Egress judged by | Memory checkpoints | Forks collide? |
| --- | --- | --- | --- | --- |
| nothing (default) | No NIC, a vsock to the node datapath | Legion policy, default deny | Yes | No: no guest address |
| `network = { mode = "gated", … }` | The same, stated explicitly | Legion policy | Yes | No |
| `config.network = { mode = "none" }` | Loopback only, no vsock, no agent | Nothing leaves | Yes | No |
| `config.network = { mode = "tap", bridge = "br0" }` | A NIC on a per-instance TAP, addressed by Legion from `br0`'s pool | The operator's bridge | Yes | No: re-addressed on every restore |

Gone: `config.network.tap` (a fixed or `"auto"` device name) and
`config.network.mac`. See §6.8.

### 6.2 A restore retargets the vsock socket to its own allocation

**The rule.** The host end of a gated sandbox's vsock is always
`<alloc dir>/.sandbox/vsock.sock`, a pure function of the allocation that
runs the guest. A restore tells the VMM to use the restoring allocation's
path instead of the one recorded in the checkpoint:

- **Firecracker:** `PUT /snapshot/load` gains
  `"vsock_override": { "uds_path": "<new alloc>/.sandbox/vsock.sock" }`,
  next to the existing `network_overrides`
  (`FirecrackerApi::load_snapshot`, `vmm_api.rs:258`).
- **cloud-hypervisor:** the restore's own copy of `config.json` gets its
  `vsock.socket` rewritten, in the same pass that rewrites fork TAPs. Rename
  `retarget_cloud_hypervisor_tap` to `retarget_cloud_hypervisor_config` and
  have it take both.

This applies to every restore of a gated checkpoint: a resume, a rollback, a
fork, a create from a snapshot, same node or another node. One code path.

**Why not a stable path.** The task brief suggested a stable per-instance
path that survives resume, reusing the device-link mechanism. The recon
rules out the device-link directory itself, and the override makes a stable
path unnecessary:

- **A disk is opened once, at load. A vsock path is resolved on every guest
  connect, for the VM's whole life.** Both VMMs connect to
  `<uds_path>_<port>` when the guest dials. The device-link directory is
  shared by every restore of one lineage, forks included, and a fork
  repoints the links. If the vsock path lived there, the origin's next
  connection after a fork would land on the fork's listener and be judged
  under the fork's identity. That is a cross-instance policy bypass.
- **A per-instance stable path** (a directory keyed by the instance id, like
  `own_tap_name`) works for a resume without any VMM support. A fork or
  create-from-snapshot still needs an override, because its instance differs
  from the origin's. It also adds an instance-keyed naming scheme to the
  datapath, a directory lifecycle nobody owns, and a dependency on identical
  worker state paths across nodes. The override covers every case with what
  the datapath already does.

So the "stable" property moves: the path is stable per allocation, known
before boot, and every restore moves the VM onto it. This mirrors the fork
TAP design, which is the mechanism we reuse.

**Fallback if Phase 0 fails.** If either override misbehaves on a real VMM,
spawn each gated VMM in a private mount namespace where a constant path
(`/run/legion/vsock/`) is a bind mount of the allocation's `.sandbox/`
directory. Every checkpoint then records the same path, and no override is
needed on any version. It costs `unshare(CLONE_NEWNS)` plus a bind mount in
the VMM's `pre_exec` (the worker already has `CAP_SYS_ADMIN` for gated
namespaces), and it is a step toward the jailer that suspend calls entry 99
already names as worth doing. Calls entry 3 records the order of preference.

**Version floor.** Firecracker below 1.16.1 cannot retarget the socket, and
1.16.0 has the restore timeout fixed in 1.16.1. Compat is an exact version
match, so checking at capture time covers every later restore. A memory
suspend, snapshot, or fork of a gated sandbox on an older Firecracker is
refused before anything is captured, the way fork TAPs are refused below
1.12:

```text
Error: instance user-42 is gated, and node w-03 runs Firecracker 1.15.0, which cannot move a
restored guest's vsock to a new socket (that needs Firecracker 1.16.1 or later)
Hint: upgrade Firecracker on the node, or use --checkpoint disk
```

The launch still boots such a sandbox ready for checkpoints, so upgrading
the VMM and restarting the instance is the whole fix. cloud-hypervisor has
no floor beyond Legion's existing one (v35, per the verification doc), with
the caveat on half-open connections in §6.3.

**The guest CID becomes a constant.** Both VMMs implement vsock in user
space over Unix sockets, so a guest CID is visible only inside its guest and
needs no host-wide uniqueness. Firecracker cannot change the CID on load and
updates it in listen sockets when it differs
([snapshot-support.md](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md)).
Today's per-allocation CID is therefore stale after every restore and shared
by every fork, to no effect. Use `3` for every gated guest and delete
`vsock_cid`. Phase 0 confirms that two guests with one CID coexist on each
VMM (cloud-hypervisor's docs list `CONFIG_VHOST_VSOCK` as a host
prerequisite, which would matter if it registered CIDs with the kernel).
The guest agent dials CID 2, the host, and never reads its own.

**The checkpoint records the network shape.** `SandboxCheckpointState`
gains `network: "gated" | "tap" | "none"`. A restore under a config whose
shape differs is refused by name, like a TAP mismatch, and
`onRestoreFailure` decides what happens next:

```text
restore needs gated networking: the checkpoint's guest has a vsock datapath and no NIC,
but task workspace is now wired to bridge br0
```

**Socket path length.** `sun_path` holds 107 bytes, and
`<worker state>/allocs/<id>/.sandbox/vsock.sock_1080` grows with the
worker's state path. Launch and restore check the length and fail by name
(`gated sandbox socket path … is 131 bytes, over the 107-byte Unix socket
limit: shorten the worker's state path`) instead of failing inside the VMM.

### 6.3 Listeners, the guest agent, and connections across a restore

**Listeners are bound before the restore.** A restore is a new allocation,
and the heartbeat that delivers it binds its proxy and resolver sockets in
`apply_upstreams` before reconcile launches it. Make the ordering a checked
precondition rather than an accident of the loop: before the restored VM is
resumed, the restore waits up to 5 seconds for `vsock.sock_1080` to exist.
If it never appears, the guest resumes anyway with no door (each dial gets a
vsock reset, which the agent turns into a closed connection), the
allocation reports `net_capture_unavailable`, and `legion net doctor` names
the unbound socket. Fail closed, never a failed restore over a transient
bind error (calls entry 34).

**In-flight connections are reset.** A memory checkpoint closes every open
guest connection:

- Firecracker sends TRANSPORT_RESET at `SnapshotCreate`. **This includes a
  live snapshot or a fork capture, so the running origin's open connections
  drop too.** An agent mid-stream on a model call sees the stream end when
  someone takes a live memory snapshot of it.
- cloud-hypervisor v52 and later reset on restore. Earlier versions can
  leave half-open sockets in the restored guest.
- On the host, the suspended allocation's proxy splices end when the killed
  VMM closes its side, and the upstream connections close with them.

The guest agent makes this uniform. It takes over the resume handler's
identity-disk watch (one process, `legion guest-agent`, calls entry 35), and
when it sees a restore it closes every relay it holds. A guest client
therefore gets a prompt connection reset on every VMM version, never a hang.
New connections dial a fresh vsock stream, which reaches the new
allocation's listener. The agent's own listeners on `127.0.0.1:1080` and
`:53` are guest-internal and survive unchanged, so the agent needs no
relisten.

The public doc states the reset rule plainly and tells harness authors to
retry an interrupted model call. `snapshot create --memory` and
`instance fork` print a one-line note when the source is a gated sandbox on
Firecracker:

```text
note: instance user-42 is gated on Firecracker, so this capture resets its open network
connections (the guest keeps running)
```

**Policy is re-evaluated at resume, by construction.** The restored guest's
traffic arrives on the new allocation's listener, the proxy evaluates
`policy_for(new allocation)` per connection, and the effective policy is
computed from the task's current config and the stored policies as they are
now. Three rules follow, all existing behavior that this plan relies on and
tests:

- A policy applied while the instance was suspended applies to the first
  connection after resume.
- Until the new allocation's policy body is on the node, every connection is
  denied `policy-pending`. A resume on the same node finds the body in the
  node cache (the hash is the same as before the suspend), so this window is
  normally empty. A cross-node resume costs one body fetch.
- A `require` policy that changed the effective mode applies at the next
  placement, which a resume is.

**Fork identity.** A fork is a new allocation with its own path, its own
listeners, and its own workload identity. The datapath attributes a
connection by **which listener it arrived on**, never by CID or any
guest-supplied value, so a fork's traffic is the fork's. The access log,
`net.denied` events, and service graph edges carry the fork's allocation id,
which `instance get` maps to its instance. This is also why forks of a gated
sandbox cannot collide on the network: there is no guest address, only
loopback ports that relay over the guest's own vsock to its own listener.

**Agent version skew.** A restored guest runs the agent binary from its
capture, which may be older than the worker that restores it. The protocol
between them is raw bytes on port 1080 and DNS-over-TCP framing on port 53,
so skew is harmless. Keep it that way: any future agent feature that needs a
real protocol must negotiate.

### 6.4 Launching gated sandboxes for checkpoints

Remove the `gated` exclusions at launch:

- `checkpointable` stops special-casing `gated` (`sandbox.rs:1140`), so a
  gated sandbox whose task asks for memory checkpoints gets device links, the
  identity disk, the balloon, and a Firecracker API socket.
- `resume_watch` stops excluding gated (`sandbox.rs:1061`). Every gated
  guest runs the agent anyway, and the agent now owns the watch.
- `checkpoint_microvm`'s refusal goes (`sandbox_checkpoint.rs:407`).

Remove the refusals at submit and at the API in the same phase:
`validate_memory_checkpoint_network` and `gated_memory_checkpoint_error`
(`src/structs/snapshot.rs:436-473`), and the gated branches in
`suspend_rpcs.rs:113` and `snapshot_rpcs.rs:210`. The Firecracker floor in
§6.2 replaces them on that VMM.

### 6.5 Gated is the sandbox default, and it denies by default

**Resolution.** `network.mode = "auto"` (the default when unset) resolves
at placement, in `effective_network_mode` (`src/storage/store.rs:12034`),
which already has the node id:

| Task | Node | Resolves to |
| --- | --- | --- |
| `sandbox`, no `config.network` | `sandbox.vmm` is `firecracker` or `cloud-hypervisor`, and `hypervisor` is not `dev` | `gated` |
| `sandbox`, no `config.network` | a dev backend (Apple `container`, Linux `dev`) | `host`, with a warning (§6.6) |
| `sandbox` with `config.network` set (`none` or `tap`) | any | `host` |
| `exec`, `container` | any | `host`, as today. Superseded for `container`: the [worker-plane auth plan](worker-plane-auth-plan.md) Phase 3 makes Linux containers gated by default |

An explicit `network.mode = "gated"` is never downgraded. On a dev backend
it still fails at start, which is the guarantee a security admin gets by
imposing `mode = "gated"` from a `require` policy. Placement reads the
node's `sandbox.vmm` attribute, which it already reads for checkpoint
support, so the cost is one attribute lookup on a path that already loads
the node row. Every sandbox allocation now gets an `AllocNetwork`, even one
whose task declares no `network` block.

An explicit `gated` plus `config.network` moves from a start failure to a
submit refusal:

```text
Error: task "workspace" sets network.mode = "gated" and config.network = { mode = "tap" }.
A gated guest has no NIC.
Hint: drop config.network to stay gated, or drop network.mode to use the TAP
```

**Default deny.** A gated allocation that no task block and no policy
object covers denies every egress connection, on every runtime. The verdict
names a new source, `gated-default`, so the `403`, the `net.denied` event,
and `legion net policy test` all say why:

```console
$ curl -sS https://example.com          # inside the sandbox
curl: (56) CONNECT tunnel failed, response 403

$ legion net policy test agents --to https://example.com
deny  gated-default  task workspace is gated and nothing allows example.com
      Hint: add "example.com" to network.egress.allow, or apply a policy object
```

Plain-HTTP requests get the existing JSON `403` body with
`Legion-Denied-By: gated-default`. An author who wants today's "observe
everything, block nothing" behavior writes it:

```nickel
network = { egress = { policy = "allow", accessLog = true } },
```

Why deny and not a friendlier default (calls entry 3):

- **Every registry an agent needs is also an exfiltration channel.** Behind
  `CONNECT`, the proxy sees a host and a port, not a method, so it cannot
  tell `npm install` from `npm publish` on `registry.npmjs.org`, or a clone
  from a push on `github.com`. A curated list that opens them by default
  opens upload paths by default.
- **Model APIs are platform-specific.** A default list cannot know which
  provider, region, or gateway a platform uses, so it is wrong for most
  users either way.
- **A list Legion ships goes stale** against where tooling actually
  connects, and the failure is a confusing `403` on a host nobody chose.
- **The previous default was loopback only.** A sandbox with no network
  config could reach nothing. Default deny keeps that reachability and adds
  the door, DNS for `*.svc.legion`, and attribution. Nothing that worked
  before stops working.

To make the deny cheap to open, ship
`configs/policies/agent-sandbox-egress.ncl`: an `enforcement = "default"`
policy object for `runtimes = ["sandbox"]` that allows the common read paths
(PyPI's `pypi.org` and `files.pythonhosted.org`, which exclude the
`upload.pypi.org` host, `registry.npmjs.org`, `crates.io` and
`static.crates.io`, `proxy.golang.org`, GitHub's clone hosts), with a comment
on each line saying what it opens, upload paths included. An operator applies
it on purpose, and `legion net policy show` names it as the source.

**Every gated sandbox gets a resolver.** `dns_specs` emits a spec for every
gated sandbox, VIP block or not (the Unix-socket variant never binds the
resolver address). Without this, a default sandbox with no upstreams has no
DNS at all. The resolver already applies egress policy at resolution time,
so a denied name is `NXDOMAIN` in one round trip. Gated `container` and
`exec` tasks without upstreams have the same gap through their namespace
resolver, recorded as a TODO rather than fixed here.

**Proxy variables that real tools read.** Set the lower-case variants
(`http_proxy`, `https_proxy`, `all_proxy`, `no_proxy`) beside the upper-case
ones, since curl ignores upper-case `HTTP_PROXY` for plain HTTP and apt reads
only lower case. Set `NODE_USE_ENV_PROXY=1`, which makes Node's built-in
`fetch` and `http` honor the variables on Node 22.21 and 24.5 or later
([Node docs](https://nodejs.org/learn/http/enterprise-network-configuration)).
Older Node ignores it and still has no route, which the public doc says.
These apply to every gated runtime.

**The agent has to run on the image.** A gated default makes the guest
agent mandatory for every networked sandbox, and today it is the worker's
glibc binary, which fails on musl images like `alpine:latest`. Phase 2
ships the guest agent and resume handler as a statically linked binary
(`legion-guest`, built for `x86_64-unknown-linux-musl` and
`aarch64-unknown-linux-musl` beside the worker), installed next to the
worker and copied into the rootfs as `/legion/legion`. The worker falls back
to copying itself when the static binary is missing, with a start warning.
Calls entry 6 records the cost: a second artifact per release.

**`none` stays, explicit.** `config.network = { mode = "none" }` means a true
air gap: no vsock device, no agent, no DNS. It is the choice for compute
sandboxes, and for images the agent cannot run on. The absence of
`config.network` now means gated, which is the one subtle line in the
surface, and the config schema table says it in the default column (calls
entry 10).

### 6.6 macOS and Linux dev mode

Apple `container` ignores `config.network` and gives each container NAT
egress through vmnet. There is no vsock into Legion's datapath, so gated
cannot be enforced, and that does not change.

- `auto` resolves to `host` on a dev backend. The worker logs a start
  warning, `instance get` shows `detail: egress-unenforced: dev mode runs
  sandbox workspace without Legion's egress policy (Apple container has no
  vsock to the datapath)`, and `legion net doctor` reports the same.
- An explicit `gated` still fails at start, as today.
- The macOS dev loop keeps working with no spec change, which is the point
  of `auto`. The agent sandboxes guide's dev-copy spec needs no edit.

Cooperative policy in dev mode (`HTTP_PROXY` pointing at a host-side proxy
on the vmnet gateway, so authors can test an allowlist on a laptop) is a
real job for the job author and an open question (§11).

### 6.7 TAP guests are addressed by Legion

**The rule.** Every TAP guest gets its address from a pool that the worker
config declares per bridge. Legion applies it at boot and re-applies a fresh
one on every restore and fork. A static IP in the image, a DHCP lease the
guest held, and the origin's address in a fork are all replaced.

```nickel
# worker config
sandbox = {
  vmmOverheadMiB = 64,
  bridges = {
    br0 = {
      subnet = "10.88.0.0/16",      # the bridge's network, for the guest's netmask
      gateway = "10.88.0.1",        # the guest's default route
      range = "10.88.128.0/17",     # what Legion leases. Keep it out of any DHCP pool
      dns = ["10.88.0.1"],          # written to the guest's resolv.conf
    },
  },
},
```

- **At boot**, the kernel command line carries
  `ip=10.88.128.7::10.88.0.1:255.255.0.0:user-42:eth0:off` (kernel IP
  autoconfiguration, no userspace tool needed), and the rootfs build writes
  `resolv.conf` from `dns`.
- **After a restore or fork**, the identity disk carries the new address,
  and the guest agent's restore step flushes `eth0`'s addresses, applies the
  new one and the default route, and flushes the neighbor table, through
  netlink from the agent binary. It replaces today's `ip link set` call in
  `set_mac`, so the image needs no `ip` tool.
- **The lease** is per allocation, held in the allocation's scratch dir
  (`guest-addr.json`, like the attached-TAP record), rebuilt from live
  allocations after a worker restart, and released on destroy. A resume on
  the same node asks for its previous address first (recorded in the
  checkpoint's runtime state) and gets it unless the range is exhausted.
- **The address is visible**: `LEGION_GUEST_ADDR` in the guest's env and
  `resume.env`, and `address: 10.88.128.7 (br0)` in `instance get`, from a
  new field on the allocation's status report.
- **Exhaustion fails the start by name**: `bridge br0's sandbox range
  10.88.128.0/17 is full (32766 addresses leased on node w-03)`.
- **Placement**: workers advertise their declared bridges as the node
  attribute `sandbox.bridges`, and a TAP sandbox gets an implicit constraint
  on its bridge, so it never lands on a node where it cannot start. Today a
  missing bridge is found at start or at fork time.

Pools are node-local. When `br0` on each node is its own L2 segment behind
host NAT, the common setup, every node can use the same range. When an
operator stretches one L2 segment across nodes, each node needs a disjoint
range, which the public doc says.

Why a Legion pool and not "re-run DHCP after restore" (calls entry 6): DHCP
re-addressing depends on a client inside the image (Debian slim has none),
on the operator running a DHCP server, and on the client honoring a restart
at the right moment, and when any of those fail, forks collide silently,
which is the bug. A Legion lease always works and makes the address
something Legion can show. Firecracker's clone recipe (a namespace and NAT
per clone, same guest IP) needs no guest change, but it takes the guest off
the operator's bridge, and L2 adjacency on that bridge is the reason to
choose TAP over gated.

### 6.8 Footguns we disallow

Each of these is a documented warning or a runtime failure today. After
this plan, each is impossible or refused before it can bite.

| Combination | Today | After |
| --- | --- | --- |
| A fixed `tap = "tap0"` on a keyed task | Refused at submit since the sandbox footguns follow-up (suspend calls entry 135), along with `count` above one and `createFirst`. A job stored before that still fails at boot (`another process holds it`) | `config.network.tap` is removed. Every TAP is per instance on `bridge`. Submit says `config.network.tap is gone: Legion creates a TAP per instance. Set bridge = "<bridge tap0 was on>"` |
| A pinned `mac` with forks | Every fork copies it, since only auto TAPs get derived MACs | `config.network.mac` is removed. Legion owns the MAC, derived from the allocation id |
| A memory fork of a TAP sandbox whose guest set a static IP | Every fork answers on the origin's IP (docs say use DHCP) | Legion re-addresses every restore from the bridge pool |
| A disk fork of a TAP sandbox | Allowed on `tap = "auto"` (each fork boots on its own `lgf` device). Refused at the API for a fixed TAP stored before entry 135, since it would boot on the origin's TAP (suspend calls entries 87, 136) | Allowed. It boots on its own TAP with its own lease |
| A TAP bridge the node does not have | Fails at start or fork time (`would have no network`) | The implicit `sandbox.bridges` constraint keeps the task off that node |
| `network.mode = "gated"` plus `config.network` | Fails at start | Refused at submit (§6.5) |
| A memory checkpoint of a gated sandbox on a VMM that cannot retarget vsock | Not reachable (all refused) | Refused before capture, with the version and the fix (§6.2) |
| Restoring a gated checkpoint into a TAP config, or the reverse | Not reachable | Refused by name, then `onRestoreFailure` |
| `connections` on a gated sandbox that can never see a connection | Accepted, never fires | Counts egress too (§6.9), and ingress after Phase 5 |
| Ingress ports on a gated sandbox before Phase 5 | Accepted, the dial fails at runtime | Refused at submit until Phase 5 ships: `ingress into a gated sandbox is not supported yet`. Phase 5 lifted it; only ports on a loopback-only sandbox are refused now (`nothing can reach a port in its guest`) |

`hypervisor = "dev"` with explicit `gated` stays a start failure rather than
a submit refusal, since only the node knows whether `auto` resolves to a
dev backend, and an explicit `dev` with `gated` is caught at submit.

### 6.9 Idle detection on gated sandboxes

Two changes, both on the egress proxy:

- **Live bytes.** Wrap both halves of every proxy splice in the activity
  ledger's `TrackedStream` (`src/worker/netd/activity.rs`), so bytes stamp
  the allocation's last-bytes time as they move. Today the proxy records
  bytes once, when the splice ends. The `network` signal then sees a long
  model stream throughout, instead of at its start and end. The hot-path
  cost is the one the ingress side already pays: a relaxed atomic store per
  read or write on a slot fetched once per connection.
- **Open egress connections.** The same wrapper counts an egress connection
  as open from accept to close. `connections` counts open ingress *and*
  open egress connections through the datapath. For a gated sandbox, that
  is the signal that keeps a harness awake through a quiet model call, which
  suspend plan TODO and calls entry 53 say no signal covers today.

`connections` stays out of the default signal list, so a sandbox holding an
idle keepalive to some service still suspends unless the author opts in.
The submit refusal for `connections` on a non-gated sandbox stays, and its
error text drops "ingress" now that the signal means both directions. The
"what each signal sees on a sandbox" table in `docs/suspend-and-resume.md`
gets a gated column that is no longer a footnote.

### 6.10 Ingress into a gated sandbox (Phase 5)

The datapath fronts an advertised port of a gated sandbox by dialing the
VMM's base socket and sending `CONNECT <private port>\n`, the host-initiated
vsock handshake both VMMs implement. Inside the guest, the agent listens on
`AF_VSOCK` for each advertised port (from the identity disk, so a restore
can change them) and relays to `127.0.0.1:<port>`. The rest of ingress
(policy, TLS, the ingress chain, `connections`) is unchanged.

After a restore, the base socket is the new allocation's path (the VMM binds
`uds_path` itself), so the same retarget covers it. Firecracker 1.17 fixes a
stuck-RX bug for host-initiated connections after a pause (#6100), which
Phase 0 measures to set this phase's floor.

This gives a platform a way to reach its sandbox (a harness API, a preview
of the dev server the agent started) without a TAP, and it makes
`connections` see ingress on gated sandboxes too.

**As built.** The handshake is `src/worker/netd/vsock.rs` (`OK <n>` or a
failed dial, two-second budget); `TaskDial::{Loopback, Vsock, Bridge}` in
`src/worker/netd/ingress.rs` is how every ingress connection reaches its
task, and HTTP mode dials through the same target. The port written in
`CONNECT` is the port the workload binds (`AllocPort::bind_port`), so the
vsock port and the guest's loopback port are one number (calls entry 15).
The boot ports reach the agent as `--ingress-port` flags in the generated
init and a restore's as `ResumeIdentity::ingress_ports`; the agent's vsock
listeners are guest memory, so they survive a restore and the identity only
adds. A restored workload keeps the ports it was captured with, so the
checkpoint's runtime state records `guest_ports` per label and the datapath
dials those, read per connection (calls entry 13). TAP sandboxes' ports are
fronted in the same change, at the leased guest address (calls entry 11).

**Upstreams, added after Phase 5.** The same agent carries a gated
sandbox's `connect.upstreams` the other way. For each upstream it listens on
`127.0.0.1:<local port>` and the VIP, and relays to host vsock port `<local
port>`, where `src/worker/forwarder.rs` binds the upstream's listener state
on a `UnixListener` (`UpstreamSpec::vsock`) instead of a loopback port, so
HTTP mode, policy, and handlers are shared (calls entry 12). The VIP makes
`<key>.svc.legion` work in the guest (calls entry 16). The checkpoint
records `guest_upstreams`, and a restore hands the agent each upstream's new
local port on the identity disk (`GuestUpstream::vsock_port`), which it
retargets every listener of that upstream to (calls entry 14). Submit
refuses a `localPort` of 1080 or 53 and a guest port of 1080 on a gated
sandbox (calls entry 38). Upstreams on TAP and loopback-only sandboxes are
deferred (calls entry 25).

## 7. Proto, struct, config, and persistence changes

- **Jobspec.** `network.mode` default resolution changes (§6.5).
  `config.network.tap` and `config.network.mac` are removed, and
  `config.network.bridge` is required with `mode = "tap"`. No new jobspec
  field. `configs/jobs/maximal.ncl` drops `tap`, `mac`, and the "memory
  snapshots do not support gated networking yet" comment, and flips the
  sandbox example to `snapshots.memory = true`. `sandbox-microvm.ncl`,
  `sandbox-edge-boot.ncl`, and `agent-sandboxes.ncl` follow.
- **Worker config.** `sandbox.bridges.<name> = { subnet, gateway, range,
  dns }` (§6.7), documented in `configs/workers/default.ncl` and
  `docs/task-runtime-sandbox.md`.
- **Structs.** `SandboxCheckpointState` gains `network` and the previous
  guest address. `ResumeIdentity` gains `guest_addr: Option<GuestAddr>`
  (address, prefix, gateway, DNS) and, for Phase 5, `ingress_ports`.
  `VsockSpec.cid` becomes a constant. `TapWiring::Fixed` goes.
- **Policy.** A new built-in verdict source, `gated-default`.
- **Proto.** The allocation status report gains `guest_address` (TAP
  sandboxes), surfaced on `InstanceInfo`. No new RPC. Proto comments on both.
- **Node attributes.** `sandbox.bridges` (list of bridge names).
- **Persistence.** None in the manager store beyond the new status field. The
  checkpoint manifest's `runtime_state` gains fields, with serde defaults so
  older manifests read as `network = "tap"` when they record a TAP and
  `"none"` otherwise. A pre-plan gated sandbox was never checkpointed, so
  nothing reads as gated by mistake.
- **Capabilities catalog and MCP.** No new commands. Update the summaries of
  `instance suspend`, `snapshot create`, and `instance fork` that mention the
  gated refusal.

## 8. Implementation phases

### Phase 0: prove gated on a real VMM (gate)

Nothing user-facing changes except the status lines that say "not
boot-tested". This phase answers the questions the design assumes.

- An opt-in boot test per VMM (`sandbox_gated_boots_firecracker`,
  `_cloud_hypervisor`, `#[ignore]` like `sandbox_microvm_boots`): the guest
  fetches an allowed URL through `HTTP_PROXY`, gets a `403` from a denied
  one, resolves a name through the relay, and fails to reach a raw IP with
  no proxy.
- Measure: single-stream throughput and connect latency through the proxy
  versus a TAP on a bridge, both VMMs, on reference hardware.
- Spikes, each recorded in the verification doc: Firecracker
  `vsock_override` on 1.16.1, cloud-hypervisor restore with a rewritten
  `vsock.socket`, two guests sharing CID 3, what each VMM does to open
  connections at capture and at restore, and the `sun_path` length on the
  default state path.
- Exit: the override path works on both VMMs, or the plan switches to the
  mount-namespace fallback (§6.2) and its calls entry is updated.

### Phase 1: gated sandboxes take memory checkpoints

- Retarget the vsock socket on every restore (§6.2), the constant CID, the
  network shape in the checkpoint, the path-length check.
- Launch gated sandboxes for checkpoints, the agent owns the resume watch
  and drops relays on restore, restore waits for the listener (§6.3, §6.4).
- The Firecracker 1.16.1 floor at suspend, snapshot, and fork.
- Remove the submit-time and API refusals, and `gated_memory_checkpoint_error`.
- Docs, same change: the gated rows and limitation bullets in
  `docs/task-runtime-sandbox.md`, `docs/suspend-and-resume.md` (Networking,
  Refusals table, Limitations), `docs/guides/agent-sandboxes.md` (Networking
  table, "A spec is rejected at submit", Limitations), and
  `docs/network-handlers.md#sandboxes` (status). Each gains the reset rule
  for open connections. Close suspend calls entry 55 with a pointer here.

Valuable alone: a platform that opts into gated gets memory suspend and fork.

### Phase 2: gated is the default for sandboxes

- `auto` resolves to `gated` for sandboxes on microVM nodes, `host` on dev
  backends with the `egress-unenforced` detail (§6.5, §6.6).
- Default deny with the `gated-default` source, for every gated runtime.
- A resolver for every gated sandbox. Lower-case proxy variables and
  `NODE_USE_ENV_PROXY`.
- The static `legion-guest` binary, with the self-copy fallback.
- Submit refusal for explicit `gated` plus `config.network`, and for ingress
  ports on a gated sandbox (until Phase 5).
- `configs/policies/agent-sandbox-egress.ncl`.
- Docs: the `network.mode` table in `docs/network-handlers.md` (default
  column), the config schema in `docs/task-runtime-sandbox.md` (`network`
  default), the agent sandboxes guide's spec (drops `config.network`), and
  `docs/agent-readiness.md` on reading `gated-default` denials.

Depends on Phase 1, since a default that refuses memory checkpoints would
break every sandbox with a `suspend` block.

### Phase 3: idle detection sees gated egress

- `TrackedStream` on proxy splices, live bytes and open egress connections
  (§6.9).
- Docs: the signals table and "What each signal sees on a sandbox".

Valuable alone for gated `exec` and `container` tasks too, whose long
outbound streams have the same blind spot.

### Phase 4: Legion addresses TAP guests

Independent of Phases 1 to 3.

- `sandbox.bridges` in worker config, the per-node lease, `ip=` at boot,
  netlink re-addressing on restore and fork, `LEGION_GUEST_ADDR`, the
  `guest_address` status field.
- Remove `config.network.tap` and `config.network.mac`. Allow disk forks of
  TAP sandboxes. Delete the fixed-TAP refusals that no longer apply
  (`ensure_boot_tap_free`, the disk-fork refusal in `fork_rpcs.rs`).
- The `sandbox.bridges` node attribute and the implicit constraint.
- Docs, same change: delete every "use DHCP in the guest" and "static IP is
  copied to every fork" passage (`docs/task-runtime-sandbox.md:339`,
  `docs/suspend-and-resume.md:1166`, `:1342`, the "Forks copy the guest's
  addresses" limitation, the failure-modes row at `:1630`,
  `docs/guides/agent-sandboxes.md:286`, `:329`, `:470`, `:1026`), and
  describe the pool. Document TAP as the escape hatch for raw networking:
  L2 adjacency, protocols the proxy cannot carry (UDP, ICMP, SSH without a
  proxy command), and bulk throughput, with egress enforced on the bridge.

### Phase 5: ingress into gated sandboxes

Status: built 2026-10-12 (G5.1 pending). See §6.10, "As built".

- Host-initiated vsock from the ingress listener, the agent's vsock
  listeners, the ports on the identity disk (§6.10).
- Lift the Phase 2 refusal of ingress ports on gated sandboxes.
- Docs: `docs/network-handlers.md` (Ingress, Sandboxes), the guide.
- Added in execution: TAP sandboxes' advertised ports are fronted by the
  same listeners, dialed at the leased guest address; the guest's ports are
  recorded in the boot record and the checkpoint so a restore dials the
  ports the restored workload holds; ports on a loopback-only sandbox are
  refused at submit instead.
- Added after Phase 5: a gated sandbox's `connect.upstreams` are relayed by
  the agent to the node's upstream listeners over vsock, VIPs and
  `<key>.svc.legion` included, and retargeted on restore. G5.2 covers it.
  TAP sandboxes' upstreams are deferred.

## 9. Testing plan

**Unit tests (macOS and Linux CI)**

- Phase 1: Firecracker restores send `vsock_override` with the new
  allocation's path, cloud-hypervisor restores rewrite `vsock.socket` and
  the TAP in one pass, a gated checkpoint refuses a TAP config and the
  reverse, the 1.16.1 floor at all three APIs (table-driven over versions),
  the path-length check, the agent closes its relays when the identity disk
  names a new allocation (with a fake `HostDial`), and the restore waits for
  the proxy socket and then resumes fail-closed.
- Phase 1 regression, the device-link trap: fork one gated checkpoint twice
  against fake VMMs, and assert each restore's vsock path is its own
  allocation's and none points into the shared `.vmdev/<key>/` directory.
- Phase 2: the resolution table in §6.5 (runtime, node VMM, `hypervisor`,
  `config.network`), `gated-default` on every gated runtime with no policy,
  `policy = "allow"` restores the old behavior, `dns_specs` for a gated
  sandbox with no upstreams, the env includes the lower-case variables, the
  submit refusals and their hints.
- Phase 3: a splice that moves bytes stamps last-bytes before it ends, an
  open egress connection counts for `connections`, and the default signal
  list ignores it.
- Phase 4: the lease (allocate, prefer previous, release, rebuild after a
  restart, exhaustion error), the `ip=` cmdline, the identity disk carries
  the address, the resume handler applies it through a fake netlink host,
  removed fields are refused at submit with the migration hint.
- `tests/integration/acl_endpoints.rs` needs no change (no new RPC), and the
  capabilities catalog test still passes.

**Integration (one manager, one worker, fake VMM API servers)**

- Suspend and resume a gated sandbox end to end through the API, with a
  stored policy applied while it is suspended, and assert the first
  post-resume connection on the new allocation's listener is denied by that
  policy.
- **Fork attribution** (the user's goal): fork a gated sandbox three times,
  drive one connection through each fork's proxy socket, and assert that the
  access log lines, the `net.denied` events, and the service graph edges
  carry each fork's own allocation id and map to its own instance. Then stop
  the origin and assert the forks' connections still work, and that no
  connection is attributed to the origin.
- **Scheduler benchmarks**: placement reads a node attribute for every
  sandbox allocation, so run `cargo bench --bench scheduler_bench` (the
  `restore` and `candidate_construction` groups) on the base branch and on
  Phase 2, and put both numbers in the PR, per AGENTS.md.

**Linux and KVM verification.** Add a "Gated sandboxes" section to
[`suspend-resume-linux-verification.md`](../suspend-resume-linux-verification.md)
in Phase 0, in that doc's format (prereqs, commands, pass and fail
criteria), and record kernel and VMM versions with each run. Prereqs:
x86_64 or aarch64 with `/dev/kvm`, Firecracker 1.16.1 or later and
cloud-hypervisor v52 or later (plus one older cloud-hypervisor for the
half-open check), a guest kernel with `CONFIG_VIRTIO_VSOCKETS=y` and
`CONFIG_IP_PNP=y`, and a bridge `br0` with NAT for Phase 4.

- **G0.1 Boot.** `sandbox_gated_boots_*`. Pass: allowed fetch succeeds,
  denied fetch gets `403` with `Legion-Denied-By`, a name resolves through
  the relay, `nc <raw ip> 443` without the proxy fails.
- **G0.2 Throughput.** Pass is a recorded number, not a threshold. The plan
  expects the proxy path to be slower than a TAP and records by how much, so
  the public doc can say it.
- **G1.1 Resume keeps a RAM-only counter and its network.** Suspend a gated
  sandbox that counts in memory and polls an allowed URL, resume it. Pass:
  the counter continues, the next poll succeeds on the new allocation (check
  `legion net doctor` and the access log for the new allocation id).
- **G1.2 Open connections reset, never hang.** Hold a slow download through
  the proxy, take a live `snapshot create --memory`. Pass on Firecracker: the
  origin's download fails promptly and a retry succeeds. On each
  cloud-hypervisor version: the origin's download continues, and a restore
  of that snapshot shows the in-guest client failing promptly (the agent's
  relay drop), never hanging.
- **G1.3 Policy at resume.** Suspend, apply a `require` policy denying the
  polled host, resume. Pass: the first post-resume poll is denied and
  attributed to the new allocation.
- **G1.4 Three forks.** Fork a gated sandbox three times on one node, and
  once with `--spread` to a second node with a published checkpoint. Pass:
  each fork's traffic is attributed to its own instance, and stopping the
  origin does not affect them.
- **G1.5 Version floor.** On Firecracker 1.15, a memory suspend of a gated
  sandbox is refused with the upgrade hint and the guest keeps running.
- **G2.1 Default.** Submit a sandbox with no network config. Pass: it boots
  gated, `pip install requests` fails with a `gated-default` denial, and
  succeeds after adding the PyPI hosts to `egress.allow`. Repeat with
  `alpine:latest` to prove the static agent.
- **G3.1 Model-call wake.** With `idle.after = "1m"` and `connections`, hold
  a connection that sends nothing for two minutes. Pass: no suspend, then a
  suspend about a minute after it closes.
- **G4.1 Re-addressing.** On `br0` with a pool, fork a TAP sandbox three
  times. Pass: four distinct addresses from the range, each matching
  `instance get`, all reachable from the host. Repeat with an image that
  sets a static IP in its own init: Legion's address wins after the fork.
- **G5.1 Ingress.** Advertise a port on a gated sandbox, `curl` it through
  the ingress listener, suspend and resume, `curl` again. Pass: both work,
  and `connections` counts the open one.

## 10. Rollout and backward compatibility

Legion has no installed base, and AGENTS.md says to break things for a
better default. What breaks, and where each PR says so:

- **Phase 2: sandboxes become gated with default deny.** A sandbox that
  declared nothing could reach nothing before and can reach nothing now,
  plus DNS and the door. A sandbox that relied on `auto` meaning `host` with
  a TAP keeps working, because a declared `config.network` resolves to
  `host`.
- **Phase 2: gated without a policy denies, on every runtime.** A gated
  `container` or `exec` task that relied on allow-all-with-attribution adds
  `egress = { policy = "allow" }`.
- **Phase 4: `tap` and `mac` are removed.** Specs move to `bridge`, and every
  worker with TAP sandboxes declares `sandbox.bridges`. The submit error
  names the replacement.
- **Firecracker floor.** Gated memory checkpoints need 1.16.1. Nodes on older
  Firecracker keep running gated sandboxes and take disk checkpoints only.

Running allocations keep the mode they were placed with until they are
replaced, since the mode is resolved at placement. A gated sandbox started
before Phase 1 was not launched for checkpoints, so it needs a restart
before its first memory checkpoint, the existing rule for any sandbox that
gains a `suspend` block. Existing TAP checkpoints restore after Phase 4 and
get a Legion address on that restore.

No feature gate. Each phase is on when it ships, and the escape hatches are
the explicit `config.network` modes and `egress.policy = "allow"`.

## 11. Open questions

- **Override or mount namespace?** Lean: the VMM overrides, because they
  reuse the fork TAP path and need no privilege. Phase 0 decides, and a
  failure on either VMM flips both to the namespace, for one code path.
- **Default deny for every gated runtime, or sandboxes only?** Lean: every
  runtime. "Gated means nothing leaves unless a rule says so" is one
  sentence, and two defaults for one mode is the kind of thing nobody
  remembers at 3am.
- **Should `connections` split into ingress and egress signals?** Lean: one
  signal, documented as both directions. Revisit if platforms report
  sandboxes kept awake by idle keepalives they cannot remove.
- **Transparent capture inside the guest.** A guest-side redirect (a default
  route to a dummy device and an `nftables` redirect to the agent, which
  forwards the original destination with SNI for the host name) would make
  proxy-unaware tools work, and it is safe because the vsock stays the only
  door. Lean: defer until Phase 2's denials show how often tools ignore the
  variables. It needs netfilter in the guest kernel and is a real project.
- **Cooperative policy in macOS dev mode.** Point the Apple container's proxy
  variables at a host proxy on the vmnet gateway so authors can test an
  allowlist locally, without enforcement. Lean: yes, as a small follow-on
  after Phase 2, since a laptop is where an author first writes the
  allowlist.
- **A curated policy shipped on by default.** Lean: no (calls entry 3). The
  shipped `agent-sandbox-egress.ncl` is opt-in.
- **Throughput.** If G0.2 shows the proxy path far below what package
  installs need, a vhost-vsock datapath or splice offload becomes a phase.
  Lean: agent traffic is registries and model APIs, and a TAP is the hatch
  for bulk data.

## 12. Acceptance criteria

- **Phase 0:** both `sandbox_gated_boots_*` tests pass on a KVM host,
  G0.1 and G0.2 are recorded in the verification doc, and the override
  spikes have a written result.
- **Phase 1:** `job validate` accepts a gated sandbox with
  `suspend.checkpoint = "memory"`. G1.1 to G1.5 pass on both VMMs. No doc in
  `docs/` says gated sandboxes cannot take memory checkpoints
  (`grep -ri "support gated networking" docs` is empty).
- **Phase 2:** a sandbox spec with no network config places as `gated` on a
  microVM node (`legion net doctor` shows `mode gated`) and as `host` with
  `egress-unenforced` on a Mac. G2.1 passes on a glibc and a musl image.
  Scheduler bench numbers are in the PR.
- **Phase 3:** G3.1 passes, and a unit test shows a mid-stream byte stamp.
- **Phase 4:** G4.1 passes, `job validate` refuses `tap = "tap0"` with the
  migration hint, and `grep -ri "dhcp" docs` finds only the operator-facing
  note about keeping DHCP pools out of the Legion range.
- **Phase 5:** G5.1 passes, and the Phase 2 ingress refusal is gone.
