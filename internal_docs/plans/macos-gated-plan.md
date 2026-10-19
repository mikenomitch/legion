# Gated networking on macOS

> **Judgement calls:** [macos-gated-plan_calls.md](../calls/macos-gated-plan_calls.md)

**Status: IN EXECUTION.** Phases 1–3 are implemented in PR #51: placement,
the confined datapath, Seatbelt for `exec`, and `pf` for Apple `container`
VMs. Every macOS command line they run is built by a pure function that is
unit-tested on Linux CI. **None of it has run on macOS hardware yet**
(phase 4), so `auto` still resolves to `host` on macOS.

Deliberately deferred:

- `auto` resolving to `gated` on macOS, until phase 4 verifies it.
- DNS inside a confined task.
- Upstreams from *ungated* Apple containers (§6.6).
- macOS 15 support for gated VMs (it has no `container network create`).
- Making the gated subnet configurable (§6.5).

## 1. Problem

On a Mac, Legion's network policy is advice:

- **`exec`.** An `exec` task is a host process. Its egress rules apply only to
  traffic it sends through Legion (its `connect.upstreams`, or a client that
  honors proxy variables Legion does not even set in host mode). A direct
  `connect()` goes out.
- **`container` and `sandbox`.** These run as Apple `container` VMs on Apple's
  own NAT network. Legion is not on their path at all: every egress rule, the
  host guard, and every egress plugin is skipped.
- **Upstreams from those VMs probably never worked.** `LEGION_UPSTREAM_*` is
  `127.0.0.1:<port>`, which inside a VM is the VM's own loopback. No code
  rewrites it, and no test covers it.
- **`network.mode = "gated"` is rejected** at start
  (`gated networking needs Linux network namespaces; this host is not Linux`).

So a developer who writes the policy for a Linux cluster cannot see it work on
their laptop, and a platform that sandboxes agent code cannot test its
confinement there.

## 2. Prior art

- **Legion on Linux.** A gated allocation gets an empty network namespace whose
  only routes are Legion's listeners (an egress proxy at `127.0.0.1:1080`,
  upstream ports, the resolver), so policy is a boundary, not advice
  ([docs](../../docs/network-handlers.md#gated-mode)). This plan gives macOS
  the same *meaning* with the mechanisms macOS has.
- **Apple `container`.** `container system dns create <domain> --localhost
  <ipv4>` lets VMs reach the Mac's `127.0.0.1`. It writes
  `rdr inet from any to <ipv4> -> 127.0.0.1` into
  `/etc/pf.anchors/com.apple.container` and loads it with `pfctl -f`
  (GHSA-39g5-644c-qwcg describes exactly that file). We take the mechanism, a
  `pf` `rdr` from a VM address to the host's loopback, but narrow it to one
  allocation's ports, because a blanket rdr would hand every VM every service
  on the Mac's loopback.
- **Apple `container` networks** (macOS 26 and later): `container network
  create <name> --subnet <cidr>` makes a separate vmnet network, isolated from
  `default`. That isolation is what lets one subnet-wide `block` confine gated
  VMs without touching anyone else's.
- **Seatbelt (`sandbox-exec`).** Chrome, the App Store sandbox, and Claude
  Code's macOS sandbox confine a process tree with an SBPL profile. Claude
  Code's network rule is the one we need: deny `network-outbound` except to
  `localhost:<proxy port>`. `sandbox-exec` is deprecated but present and
  working through current macOS. The replacement, a Network Extension
  content filter, needs a signed and notarized system extension and user
  approval, so it is rejected for a dev-mode worker (calls entry 3).
- **Docker Desktop.** `host.docker.internal` and vpnkit give containers a path
  to the host; Docker does not enforce egress policy at all. We need both.

## 3. Jobs to be done

- **A developer on a Mac.** "I wrote `network.mode = "gated"` and an egress
  allowlist for production. I want to run the same job on my laptop and see
  the same allow and deny, not a different behavior I discover in staging."
- **A platform building agent sandboxes.** "My product runs untrusted code in
  `sandbox` tasks. On a Mac I need to prove that code cannot reach anything
  but my proxy, and test my egress plugins against it."
- **An operator.** "If a Mac worker cannot confine a gated task, I want the
  task to fail to start with the reason, never run unconfined."

Scenarios the design has to serve:

1. `legion start manager --run-worker` on a Mac, then a gated `exec` job that
   runs `curl https://example.com`. It gets a `403` from Legion's proxy
   unless the policy allows it. A `python -c 'socket.create_connection(...)'`
   that ignores the proxy gets "Operation not permitted".
2. The same machine runs a gated `container` job. Its `HTTPS_PROXY` points at
   Legion. Its `$LEGION_UPSTREAM_DB` reaches the `db` service. A raw dial to
   `1.1.1.1` is dropped.
3. A Mac worker running as a normal user is given a gated `container` job.
   The allocation fails with "gated Apple containers need the worker to run
   as root to load the pf rules that confine them".

## 4. Goals and non-goals

**Goals:**

- `network.mode = "gated"` means on macOS what it means on Linux. The
  datapath is the only way out, it fails closed while policy is pending, and
  the host guard and egress chain judge every flow.
- The same jobspec, with no macOS-only fields.
- Upstreams work from Apple `container` VMs.
- Every confinement step that fails is a failed start that names the step.

**Non-goals:**

- An enforced default on macOS (`auto` stays `host` until phase 4).
- Transparent capture of tools that ignore the proxy. On Linux the eBPF
  classifier does it; on macOS such a tool simply has no route out.
- Per-allocation DNS. A confined task resolves names through the proxy, as a
  gated Linux task without upstreams does.
- Network Extensions, kernel extensions, or anything needing notarization.
- UDP egress (Linux gated has none either).

## 5. Background: what exists today

- **Mode resolution.** `resolve_network_mode_with`
  (`src/structs/network.rs:104`) keeps `auto` at `host` on macOS. An explicit
  `gated` is placed, then refused at start by `netns::create`
  (`src/worker/netd/netns.rs:114`) through `ensure_gated_namespace`
  (`src/task_plugins/mod.rs:1056`).
- **Placement.** `assign_allocation_network` (`src/storage/store.rs`) gives a
  gated namespace a resolver and private twins for every port, and upstreams
  loopback ports when the node has no VIP block (macOS never does).
- **The datapath.** It binds where `Attachment` says
  (`src/worker/netd/attach.rs`): `Host`, `Netns`, `GuestVsock`, `GuestTap`.
  The gated proxy listens at `127.0.0.1:1080` inside the namespace
  (`proxy_specs`, `src/worker/netd/specs.rs:138`). The proxy env vars name
  that fixed address (`AllocNetwork::env_pairs`).
- **Launches.** `exec` spawns its command directly
  (`src/task_plugins/exec.rs:592`). Apple `container` runs are built by
  `build_apple_launch` in `container.rs` and `sandbox.rs` (dev mode). Neither
  passes `--network`, so every VM lands on `default`.

## 6. Design

### 6.1 One meaning, two mechanisms

The user writes what they write today:

```nickel
network = { mode = "gated", egress = { allow = ["pypi.org", "files.pythonhosted.org"] } },
```

On a macOS node that allocation is **confined**. Its datapath listeners bind
on the Mac's loopback at ports placement chose, and the runtime keeps the
task from reaching anything else:

| Runtime | Confinement |
| --- | --- |
| `exec` | a Seatbelt profile: outbound only to the allocation's own listener ports on `localhost` (§6.4) |
| `container`, `sandbox` (Apple `container`) | the gated vmnet network, where `pf` drops everything from the VM except `rdr`s to the allocation's own listener ports on the host (§6.5) |

### 6.2 Placement

For a gated allocation on a node whose `os` is `macos`:

- **An egress proxy port.** One dynamic port from the node's port space,
  recorded as `AllocNetwork.egressProxy = "127.0.0.1:<port>"`. It is the
  signal everything downstream keys on: an allocation is confined exactly
  when it is gated and has an `egressProxy`.
- **Private twins for every declared port**, as for a gated namespace. They
  are host ports, though, because there is no namespace to keep them in. The
  datapath fronts every advertised port.
- **No resolver.**
- **Upstreams get loopback ports, as on any macOS node.**

### 6.3 The datapath: `Attachment::Confined`

A new attachment for confined allocations:

- Listeners bind on the node's loopback, as in host mode.
- The proxy binds at `egressProxy`, not `:1080`.
- It **is gated**: the gate and policy-pending checks fail closed
  (`netd::egress::gate_block`), and the proxy's `gated-default` applies.
- No namespace is created, and `ensure_gated_namespace` skips it.
- The env vars name the allocation's own proxy:
  `HTTP_PROXY=http://127.0.0.1:<port>`.

### 6.4 `exec`: Seatbelt

A confined `exec` task runs as
`/usr/bin/sandbox-exec -p <profile> <command…>`, with this profile:

```scheme
(version 1)
(allow default)
(deny network-outbound)
(allow network-outbound (remote unix-socket))
(deny network-outbound (remote unix-socket (path-literal "/private/var/run/mDNSResponder")))
(allow network-outbound (remote ip "localhost:20017"))   ; the egress proxy
(allow network-outbound (remote ip "localhost:20018"))   ; each upstream's loopback port
(deny network-bind (local ip "*:*"))
(allow network-bind (local ip "localhost:*"))
```

- **Outbound is the allocation's own listeners and nothing else.** That
  excludes the manager's `:4747`, other allocations' ports, and the LAN. The
  host guard's loopback rule falls out of it.
- **No DNS.** The process cannot reach `mDNSResponder`, so it cannot
  resolve names itself (or tunnel data through queries). Proxy-aware tools
  send the name to the proxy.
- **Binds only on loopback.** The task's listeners are reachable only
  through the datapath's ingress listeners, which front every port (§6.2).
- **It covers the whole process tree.** `sandbox-exec` applies the profile
  and `exec`s the command, so the PID Legion supervises is the task.
- **A missing `/usr/bin/sandbox-exec` fails the start** with that reason.

### 6.5 Apple `container` VMs: the gated network and `pf`

The worker owns one Apple `container` network, `legion-gated`, on
`192.168.239.0/24`; its gateway `192.168.239.1` is the Mac. For a confined
`container` or `sandbox`:

1. **The network.** It is created on first use (`container network create
   legion-gated --subnet 192.168.239.0/24`). This needs macOS 26 or later; on
   macOS 15 the start fails, saying so.
2. **The base rules.** `pf` is enabled (`pfctl -E`, keeping its reference
   token) and the anchor `com.apple/legion` is loaded. macOS's stock
   `/etc/pf.conf` evaluates `com.apple/*` for both translation and filter
   rules, so no system file is edited. The base rules allow DHCP to the
   gateway and drop every other packet from the subnet:

   ```
   pass in quick inet proto udp from 192.168.239.0/24 to any port 67
   block drop in quick inet from 192.168.239.0/24 to any
   ```

   A VM is therefore confined **before** Legion knows its address: until its
   own rules land, it reaches nothing.
3. **The launch.** The container runs with `--network legion-gated`.
   Datapath addresses in its environment (`LEGION_UPSTREAM_*`,
   `LEGION_EGRESS_PROXY`, the proxy variables) name the gateway instead of
   `127.0.0.1`.
4. **The VM's rules.** The worker reads the VM's address from `container
   inspect <name>`, then loads the anchor again with one `rdr pass` per
   confined VM, ahead of the base rules:

   ```
   rdr pass inet proto tcp from 192.168.239.5 to 192.168.239.1 port { 20017 20018 } -> 127.0.0.1
   ```

   `rdr pass` skips filtering, so only those ports reach the Mac, and they
   land on the allocation's own listeners on `127.0.0.1`. Replies to
   connections the Mac opened (the published ports, health checks) match
   `pf` state and pass.
5. **Teardown.** A stopped allocation's rule is removed and the anchor
   reloaded. When the last confined VM is gone, the anchor is flushed and
   `pf`'s reference is released (`pfctl -X <token>`).

**Root is required.** `pf` needs root, and a dev-mode worker usually is not:
the start fails with "gated Apple containers need the worker to run as root
to load the pf rules that confine them".

**The subnet is a constant.** That is the deferred configuration knob (calls
entry 4).

### 6.6 What this does not fix

**Ungated Apple containers.** `LEGION_UPSTREAM_*` still names `127.0.0.1`
inside the VM. The same `rdr`, without the block, would fix them on
`default`, but it needs root for a mode that today needs nothing. It is a
TODO with this plan as its design, not part of it.

### 6.7 Failure modes

| Failure | What the user sees |
| --- | --- |
| `sandbox-exec` missing | `allocation <id> is network.mode = "gated" but /usr/bin/sandbox-exec is missing, so it cannot be confined on macOS` |
| macOS 15, no `container network create` | `gated Apple containers need macOS 26 or later (container network create failed: …)` |
| Worker not root | `gated Apple containers need the worker to run as root to load the pf rules that confine them` |
| The VM never reports an address | the VM stays confined by the base block (it reaches nothing) and the worker logs a `WARN` every 10s for 60s. It is not failed: the task runs, unable to reach anything, which phase 4 may change into a failed start |
| Policy pending, gate shut | the proxy and upstreams refuse, as on Linux |

## 7. Struct and config changes

- `AllocNetwork.egress_proxy: Option<String>` (`egressProxy`), which is wire
  and placement data, not jobspec.
- No new jobspec field: `network.mode = "gated"` already exists. Nothing goes
  into `configs/jobs/maximal.ncl`.
- No new RPC, and no proto change. `TaskAllocation.network` is JSON.
- No worker config yet. The subnet, network name, and anchor are constants.

## 8. Phases

1. **Placement and the confined datapath.** Ships on its own: a confined
   allocation's proxy and listeners fail closed and name the right address.
2. **`exec` under Seatbelt.**
3. **Apple `container` VMs under `pf`** (`container` and dev-mode `sandbox`).
4. **Verification on macOS hardware**, on macOS 26 with a root worker:
   - the three scenarios of §3, recorded with timings;
   - fix what they find;
   - then flip `auto` to `gated` for `container` and `sandbox` on macOS 26
     nodes.

## 9. Testing

- **Unit, on Linux CI.** Each of these is a pure function with tests:
  - the SBPL profile from the allowed ports;
  - the `pf` anchor text from the confined VMs;
  - `container inspect` address parsing (from a recorded sample);
  - the environment rewrite;
  - `Attachment::of` for confined allocations;
  - placement's proxy port and twins on a `macos` node.
- **Integration, on Linux.** A confined allocation's proxy fails closed until
  its policy arrives, as gated does.
- **On macOS (phase 4).** The §3 scenarios, plus a VM that dials `1.1.1.1`
  directly and a second confined VM dialing the first one's ports.

## 10. Rollout

Opt-in by construction: only an explicit `mode = "gated"` on a macOS node
takes the new path, and that was a start failure before. Nothing about a host
mode task changes. The `pf` anchor is Legion's own, under `com.apple/legion`,
and is flushed when the worker stops.

## 11. Open questions

1. **Does `rdr pass` to `127.0.0.1` from a vmnet bridge work without
   `net.inet.ip.forwarding`?** Apple's own `--localhost` rule suggests it
   does. Phase 4 decides.
2. **Does vmnet's own DNS proxy on the gateway answer on port 53 in the gated
   network?** If it does, the block already hides it. Phase 4 checks that a
   confined VM's `dig` fails.
3. **IPv6.** The gated network is created IPv4-only. If vmnet still hands
   out an IPv6 address, the base rules need an `inet6` block on the bridge,
   whose name only the running system knows. Lean: read it from `container
   network inspect` in phase 4.

## 12. Acceptance criteria

- **Phase 1.** A gated allocation placed on a `macos` node has an
  `egressProxy` and twins for every port. Its proxy refuses with
  `policy-pending` until policy arrives. Its env vars name its own proxy.
- **Phase 2.** The profile allows exactly the allocation's listener ports, and
  the launch is `sandbox-exec -p …`.
- **Phase 3.** The launch has `--network legion-gated`, datapath env vars name
  the gateway, and the anchor has one `rdr pass` per confined VM ahead of the
  block.
- **Phase 4.** On a Mac:
  - scenario 1 shows the `403` and `EPERM`;
  - scenario 2 reaches the upstream and cannot reach `1.1.1.1`;
  - scenario 3 fails with its message.
