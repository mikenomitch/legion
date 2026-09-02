# Network Handlers: Egress and Ingress Policy, Upstreams, and Middleware

Legion's node datapath — the loopback forwarder behind `connect.upstreams` —
can decide, observe, and (where you ask) transform the traffic that crosses a
task's boundary. This page is the reference for the three concepts that make
that possible and the surfaces they are used through:

- **Policy** — declarative rules that say what a task may reach and who may
  reach it. Written inline on a task, or stored as a **policy object** that
  applies to every task a selector matches (a node pool, a runtime, a job
  glob, a label) and changes live, without restarting anything.
- **Upstreams** — where a task's egress goes. A catalog service, as before,
  or an **external destination** to which Legion originates TLS, so the task
  speaks plaintext to a loopback port and the body is visible to policy
  without anyone's TLS being intercepted.
- **Handlers** — code on the path, only when policy is not enough: a webhook
  that returns a verdict, or any proxy in any language.

The design and the phases are in
[`internal_docs/plans/service-mesh-plan.md`](../internal_docs/plans/service-mesh-plan.md)
and [`network-handlers-plan.md`](../internal_docs/plans/network-handlers-plan.md).
This page describes **what ships today** and says plainly what does not
yet; see [Current scope](#current-scope-and-limitations).

## Quick start

Deny everything but two registries, on the connect datapath, for one task:

```nickel
{
    name = "agent",
    tasks = [{
        name = "sandbox",
        runtime = "exec",
        config = { command = ["sh", "-c", "echo $LEGION_UPSTREAM_PYPI; sleep 3600"] },
        network = {
            egress = { allow = ["pypi.org", "files.pythonhosted.org"] },
        },
        connect = {
            upstreams = [
                { name = "pypi", to = "https://pypi.org" },
                { name = "files", to = "https://files.pythonhosted.org" },
            ],
        },
    }],
}
```

Or once, for every sandbox on a pool, with nothing in the job:

```bash
legion net policy apply -f configs/policies/sandbox-egress.ncl
legion net policy show agent              # what the task actually gets, with sources
legion net policy test agent --to https://api.github.com/user   # would this be allowed?
```

## What the datapath does today

Every connection a task makes through an upstream — `LEGION_UPSTREAM_<KEY>`,
a loopback port the node owns — passes through the node's forwarder, which
runs in one of two modes:

**L4 (the default).** Before dialing, the forwarder evaluates the task's
**effective egress policy** with what is known at connect time: the
destination host (the service name, or an external upstream's host), the
port, and for service upstreams the endpoint about to be dialed. A denial
closes the connection without dialing, increments `legion_net_denied_total`,
and is reported on the next heartbeat as a `net.denied` event naming the
rule. An allowed connection is spliced; nothing is parsed. That is the whole
per-connection cost of a connect-time policy — one rule walk, no bytes
inspected.

**HTTP mode.** An upstream is parsed as HTTP/1.1 when it declares HTTP — an
`http://`/`https://` target, an `http` block, or a `dlp` handler — or when
the effective egress policy is `protocol = "http"`. Per request:

1. The chain is evaluated with the request head (method, path, headers) on
   top of the connect-time facts. A denial is a `403` whose body and
   `Legion-Denied-By` / `Legion-Denied-Reason` headers name the rule, so a
   client — an agent in a sandbox — reads *why* instead of retrying.
2. `dlp` handlers buffer the body up to `maxBytes` and reject a match.
3. Traffic management from the upstream's `http` block: a per-request
   `timeout`; `retries` for idempotent methods only (by default) on
   connect failures and 5xx, under a `budget` (default 20% of requests per
   minute); a weighted `split` by service tag; `outlier` ejection of an
   endpoint after `consecutive5xx` failures for `ejectFor`.
4. One upstream connection per request, TLS originated for `https://`, the
   `Host` header rewritten to an external target's host.

`accessLog = true` on the chain (or a `log` handler) writes one NDJSON line
per request to `<worker logs>/<allocation>/net-access.log`, carrying the
task's labels, the verdict, the deciding rule, status, bytes, and duration.

HTTP mode is HTTP/1.1 only: no HTTP/2, no WebSocket upgrades. A task that
needs those uses a `tcp://`/`tls://` upstream and an L4 policy.

## Policy

### The inline form

`network.egress` and `network.ingress` on a task. `allow` is sugar; the full
form has `rules`, `handlers`, and `tls`.

```nickel
network = {
    labels = { user = "alice", tier = "free" },   # attribution; selectors can match on these
    egress = {
        protocol = "tcp",       # tcp (default) | http — never inferred from the rules
        policy = "deny",        # fallthrough: allow | deny (deny when `allow` is used)
        onError = "deny",       # what a failed handler means: deny | allow
        accessLog = false,
        allow = ["*.pypi.org", "api.github.com:443", "10.0.0.0/8:5432"],
        rules = [
            { when = { host = "telemetry.example.com" }, action = "allow" },
            { when = { cidr = "169.254.0.0/16" }, action = "deny", reason = "no metadata service" },
        ],
        handlers = [],
    },
},
```

**Rules match on** `host` (a glob: `*` matches one or more labels at a label
boundary, so `*.pypi.org` matches `files.pypi.org` and never `pypi.org`;
bare `*` matches everything), `port`, `cidr` (the resolved destination or the
ingress peer), `identity` (the verified peer's workload identity, ingress),
and — with `protocol = "http"` — `path` (`*` within a segment, `**` across),
`method`, and `header` (name → value glob). Every present field must match;
first match wins; `policy` is the fallthrough. A `deny` rule's `reason` is
returned to the workload and recorded on the event.

**`allow` entries** are `host`, `host:port`, `cidr`, `cidr:port`, an IP, or —
only with `protocol = "http"` — `host/path`. A path in a `tcp` chain is
rejected at submit with the fix:

```
egress.allow[1] "api.github.com/repos/**" matches on a path, but
egress.protocol is "tcp". Set protocol = "http" to match on paths, or
drop the path to match the host only.
```

`protocol` is never inferred. A rule set that silently became L7 because
someone typed a slash would be a policy you cannot review by reading, and
about twice as expensive per byte.

### Policy objects

A stored policy is the same block plus a **selector** and an **enforcement**
mode. It lives in the cluster store (raft-replicated), is edited with
`legion net policy`, and applies to every matching task on its next
heartbeat — no restart, no rollout, no job edit.

```nickel
# configs/policies/sandbox-egress.ncl
{
    name = "sandbox-egress",
    "priority" = 100,
    selector = {
        nodePools = ["untrusted"],       # node.pool globs
        namespaces = ["default"],        # every job is in `default` until namespaces land
        runtimes = ["sandbox"],
        jobs = ["agent-*"],              # job id globs
        labels = { tier = "free" },      # task network.labels that must all match
    },
    enforcement = "require",             # require | default
    mode = "gated",                      # optional network.mode to impose
    egress = { protocol = "http", policy = "deny", allow = ["*.pypi.org", "api.github.com/repos/**"] },
}
```

`selector = {}` matches every task on the cluster. Every list is an OR, the
fields are an AND.

| `enforcement` | Applied when | A job author may |
| --- | --- | --- |
| `default` | the task declares no chain of its own for that direction | replace it entirely by declaring one |
| `require` | always, **ahead of** the task's own block | append rules and handlers, add denials; never remove or widen |

**The merge is deterministic and shows its work.** Every matching `require`
policy applies in `priority` order (ties by name), then exactly one of: the
task's own block, or the highest-priority matching `default` policy. A
required `deny` fallthrough is never widened by anything after it. Two
`default` policies that would tie for the same task — equal priority,
overlapping selectors — are rejected at `apply`, so the winner is never
decided at runtime. In the merged result every rule and handler carries a
`source`: `policy/<name>#<index>` or `task#<index>`.

`legion net policy show <job>` prints exactly that merged result; the node
datapath runs the same function on the same inputs.

### How policy reaches a node

The heartbeat reply carries one content hash per allocation. A node fetches
a body it has not seen once and caches it, so a thousand identical sandboxes
cost one fetch. Bodies are swapped only after a successful heartbeat and the
last known good body is kept through a manager outage — the same fail-static
contract as the endpoint feed. A cluster with no stored policy and no task
declaring one pays nothing per heartbeat.

## Upstreams

`connect.upstreams` entries are either a catalog **service** or an
**external destination**:

```nickel
connect = {
    upstreams = [
        { service = "db" },                                   # as before
        { name = "openai", to = "https://api.openai.com" },  # external
    ],
},
```

An external upstream is `name` + `to`; the task gets `LEGION_UPSTREAM_<NAME>`
(`127.0.0.1:<port>`) exactly like a service upstream. The scheme of `to`
decides two things:

| `to` | Legion originates TLS | HTTP framing |
| --- | --- | --- |
| `tcp://host:port` | no | no |
| `tls://host:port` | yes | no |
| `http://host[:port]` | no | yes (HTTP phase) |
| `https://host[:port]` | yes | yes (HTTP phase) |

With `https://`, the task speaks **plaintext HTTP to loopback** and the
node's forwarder dials the destination over TLS (verified against the
Mozilla roots). That is how an LLM API call's body becomes visible to policy
and handlers **without anyone's TLS being intercepted**: point the client at
the upstream —

```sh
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
```

— and Legion never holds a certificate authority that can impersonate the
public internet. `to` must not carry a path or query; the task chooses paths.

The `http` block on an upstream (`timeout`, `retries`, `split`, `outlier`)
turns on HTTP mode with traffic management; per-upstream `handlers` run
there too. See [What the datapath does today](#what-the-datapath-does-today).

## Name-is-config: `<key>.svc.legion`

On Linux, every allocation that declares upstreams is assigned a loopback
/24 of its own at placement (`127.42.<b>`, unique per node — the whole 127/8
is loopback on Linux with no configuration). In it:

- **`.1` is the allocation's resolver**, exported as `LEGION_DNS`. It answers
  `<key>.svc.legion` (and bare `<key>`, via the `svc.legion` search domain)
  with that allocation's upstream VIP; SRV carries the port. Everything else
  is forwarded to the host's resolvers — after the egress policy has judged
  the name, so a host the policy would deny is `NXDOMAIN` in one round trip
  and a `net.denied` event (`protocol: dns`), never a hanging connect. Names
  are not judged when the chain matches on CIDRs (the address is what the
  rule needs).
- **`.2` onward are the upstream VIPs**, one per `connect.upstreams` entry in
  declaration order, each listening on the upstream's **canonical port**: a
  pinned `localPort`, else the port the service declared on its own task
  (`network.ports[].to`, or its static port), else an external target's
  port, else the dynamic loopback port. `db.svc.legion:5432` works because
  the `db` service declared 5432.

Both addresses reach the same listener state, so policy, HTTP mode, and
endpoints are identical whichever one the task dials. The loopback port and
`LEGION_UPSTREAM_<KEY>` are unchanged; the name is additive.

| Env var | Value |
| --- | --- |
| `LEGION_DNS` | `127.42.<b>.1` — the allocation's resolver |
| `LEGION_UPSTREAM_HOST_<KEY>` | `<key>.svc.legion` |
| `LEGION_UPSTREAM_VIP_<KEY>` | `<vip>:<canonical port>` |

**Who gets the resolver automatically.** A `container` task's `/etc/resolv.conf`
is generated to point at it (`search svc.legion`, `ndots:1`). An `exec` task
gets `LEGION_DNS` and keeps the host resolver unless it opts in — a hardened
`exec` with a private mount namespace will get the generated file in the
gated phase. A gated `sandbox` gets the guest agent's relay as its resolver
(`nameserver 127.0.0.1` in the guest's `resolv.conf`; see "Sandboxes").

**macOS** configures only `127.0.0.1` on `lo0`, so no VIP block is assigned
there: upstreams keep their loopback ports and env vars, and there is no
per-allocation resolver. Documented, not hidden.

## Handlers

Handlers are code on the path and appear only when declared, on a chain
(`egress.handlers`, `ingress.handlers`, on a task or a policy object) or on
one upstream (`connect.upstreams[].handlers`). They run in order after the
rules have allowed the flow; the chain's hops first, then the upstream's.

| `handler` | What it does | Sees bytes |
| --- | --- | --- |
| `authz` | POSTs the flow's facts to `url` and reads back a verdict, cached per (hop, identity, destination[, method, path]) for `cache` (default `10s`, at most `5m`) | no |
| `proxy` | hands the flow to any proxy — a catalog `service`, its `node`-local endpoints, or a sidecar's `port` label in the same job — with a PROXY protocol v2 header (TCP) or `Legion-*` headers (HTTP) | yes |
| `log` | access log for this hop | as declared |
| `dlp` | rejects HTTP bodies matching `config.patterns` up to `config.maxBytes` (needs `protocol = "http"`) | yes |

Every hop has a `timeout` (default `2s`) and an `onError` (`deny` by
default). There is no syntax for an unbounded handler, and a hop that
cannot be reached — a webhook that times out, a proxy with no endpoint —
fails the chain **closed** unless it says `onError = "allow"`.

```nickel
egress = {
    protocol = "http",
    allow = ["api.github.com/**"],
    handlers = [
        { handler = "authz", url = "http://policy.internal:8080/check", cache = "30s" },
        { handler = "proxy", service = "egress-filter", onError = "deny" },
    ],
},
```

### `authz` — ask your service

The webhook is what most custom logic actually is: "may *this* workload
talk to *that*?" Legion asks it once per new (identity, destination) pair
and caches the answer.

**Request** (`POST`, `application/json`, header `Legion-Handler: authz`):

```json
{
  "allocation": 41, "job": "agent-7", "task": "sandbox",
  "identity": "auto:agent-7:default:sandbox",
  "labels": { "user": "alice", "tier": "free" }, "node": "node-3",
  "direction": "egress", "protocol": "http",
  "destination": "api.github.com:443", "host": "api.github.com", "port": 443,
  "peerIdentity": null,
  "method": "GET", "path": "/repos/x", "headers": [["host", "api.github.com"]]
}
```

`method`, `path` and `headers` are present in HTTP mode only; `authorization`,
`cookie` and `proxy-authorization` are never forwarded. `peerIdentity` is
the verified client on ingress.

**Reply** (`200`): `{ "verdict": "allow" | "deny", "reason": "...", "ttl": "1m" }`.
Anything else — a non-2xx status, a body that is not JSON, a verdict that is
not `allow`/`deny`, a timeout — is a handler error and takes `onError`. A
`deny` is a policy denial like any other: the connection closes (or the
request gets a `403` naming the hop as `Legion-Denied-By`), a `net.denied`
event carries the reason, and `legion net policy show` names the hop.

**Where it runs.** On L4 paths (an upstream splice, the gated egress proxy's
`CONNECT`/SOCKS, an ingress splice) the webhook is called at connect time
with the connect-time facts. In HTTP mode it is called per request with the
request head — one call per new (identity, destination, method, path), then
the cache.

### `proxy` — bring your own proxy

A `proxy` hop takes the flow. Legion dials the proxy and hands it the
connection with everything it needs to decide or filter, then splices:

- **TCP** (an L4 upstream, `CONNECT`, SOCKS5, an ingress splice): the
  connection starts with a **PROXY protocol v2** header — source and
  destination addresses, plus Legion TLVs `0xE0` identity, `0xE1` the
  original destination as `host:port` (or the task's private port on
  ingress), `0xE2` allocation id, `0xE3` labels (`k=v;...`), `0xE4`
  direction, `0xE5` job, `0xE6` task. HAProxy, Envoy, nginx and Caddy read
  the addresses and ignore the TLVs they do not know; `proxyProtocol = false`
  omits the header for a proxy that cannot take one.
- **HTTP** (an HTTP-mode upstream, an absolute-URI request through the
  gated proxy): the request reaches the proxy in **absolute form**
  (`GET http://api.example/x HTTP/1.1`) with headers `Legion-Identity`,
  `Legion-Allocation`, `Legion-Job`, `Legion-Task`, `Legion-Labels`,
  `Legion-Direction`, `Legion-Destination`, and `Legion-Peer-Identity` on
  ingress. The proxy answers for the origin; a `4xx` from it is the denial.

Targets: `service = "x"` (any healthy endpoint of catalog service `x`,
round-robin), `node = "x"` (only endpoints of `x` on the same node — a
per-node filter run as a system job), or `port = "filter"` (a sidecar task in
the same job with a port of that label, on this node). The manager feeds
the endpoints on the heartbeat like an upstream's; a target with no endpoint
is a handler error (`onError`).

An ingress `proxy` hop hands the *inbound* connection to the proxy, which
reaches the task on the private port named in the header; that works for a
task in host mode (the proxy can dial the port) — a gated task's private
port is inside its namespace, which an external proxy cannot enter.

### Health, and `legion net handler test`

Every hop the datapath has run is reported on the heartbeat
(`NetAllocStatus.handlers`: source, kind, target, healthy, calls, errors,
last error) and shown by `legion net doctor` and under **Datapath** in the
UI's task Network section. Before wiring a handler in:

```bash
legion net handler test http://policy.internal:8080/check   # authz: sample facts → verdict?
legion net handler test 10.0.0.9:9000                       # proxy: accepts a PROXY v2 header?
```

The probe sends one synthetic request the way the datapath would and prints
each check (`reachable`, `status_2xx`, `verdict`, `ttl`; or `proxy_protocol`)
with a fix. Non-zero exit when a check fails. Also an MCP tool
(`legion_net_handler_test`).

## mTLS

Legion terminates and originates TLS; a handler never sees it. Two kinds of
material, chosen per chain:

```nickel
ingress = {
    tls = {
        mode = "mutual",                      # off | server | mutual
        certFrom = "legion",                  # the mesh: this node's certificate
        clientCa = "legion",                  # verify peers against the mesh CA
        requireIdentity = ["auto:web:*"],     # workload globs a peer must match
    },
},
```

| Field | Meaning |
| --- | --- |
| `mode` | `server` presents a certificate; `mutual` also **verifies** the client's. Egress: `passthrough` (default, opaque) or `originate` (external upstreams; TLS from the node, plaintext from the task). |
| `certFrom = "legion"` | the node's mesh certificate (below). Omit it and give `certPath`/`keyPath` for bring-your-own files on the node (or staged artifacts). |
| `clientCa` | `"legion"` (the mesh CA) or a CA bundle path; required for `mutual`. |
| `requireIdentity` | identity globs; a verified peer outside them is refused with `Legion-Denied-By: tls/requireIdentity` and a `net.denied` event. Empty = any verified peer. |
| `caPath` | egress `originate`: verify the destination against this bundle instead of the system roots. |

**Bring-your-own PKI.** With `certPath`/`keyPath` (and `clientCa` for
`mutual`), the advertised port terminates TLS with your certificate and a
`mutual` peer's identity is its certificate's first URI SAN, else its first
DNS SAN, else its CN — that string is what `identity` rules and
`requireIdentity` match.

**The mesh (`certFrom = "legion"`).** The manager keeps one cluster **mesh
CA** (created on first use, replicated with the control lane) and issues each
worker node a short-lived **node certificate** — SANs `<node>.node.legion` and
`legion://node/<node id>`, valid 24 hours, refreshed by the worker after 8 —
over the authenticated worker channel (`IssueNodeCertificate`). Nothing is
written to disk on the node. Then:

- A task whose ingress chain says `mode = "mutual", certFrom = "legion"` has
  its advertised port terminate **node-to-node mTLS**. The catalog marks the
  endpoint `mesh_tls`; every consumer's datapath dials it with its own node
  certificate, verifies the peer node, and names the consuming **workload**
  (`<region>:<job>:<group>:<task>`) in a PROXY v2 TLV inside the session.
  The receiving node trusts that attribution because the peer proved it is a
  node, and feeds it to `identity` rules, `requireIdentity`, and `authz`.
- A plain client dialing the port directly is refused at the handshake. A
  non-Legion client that must reach it presents a certificate from the mesh
  CA (`legion net ca show` prints the CA; certificates for outsiders are not
  issued by Legion) or goes through a Legion task.
- Workloads never hold certificates: identity is attributed by the node,
  which is what keeps a per-user sandbox's identity out of the sandbox.

`legion net ca show` prints the CA certificate (never the key);
`legion net ca rotate` replaces it — every node re-issues on its next
heartbeat, and until it has, peers on different CAs refuse each other. Never
plaintext.

**Doctor.** `legion net doctor` reports `tls` as a layer: BYO files that do
not parse, or a mesh certificate the node has not been issued yet (mesh peers
are refused until then, and the allocation's `problems` say so).

Limits: no kTLS/`splice` fast path yet — a terminated mesh connection is
copied in user space; TLS 1.2 and 1.3; no certificate revocation (the
24-hour lifetime is the revocation).

## `network.mode`

| `mode` | Meaning | Default for |
| --- | --- | --- |
| `host` | host networking; policy is cooperative (proxy env vars, upstreams, the resolver) | `container`, `exec`, `sandbox` |
| `gated` | the workload's network namespace holds loopback and the datapath only — **unbypassable** on Linux for `container` and `exec`; for a `sandbox`, a microVM with no NIC whose only way out is a vsock to the datapath (see "Sandboxes") | — |
| `auto` | `host` today for every runtime; the sandbox default flips to `gated` once the vsock transport has been exercised on a real VMM in CI | — |
| `bridge`, `cni/<name>` | reserved for the datapath plugin; rejected at submit | — |

The effective mode is resolved **at placement**: the task's own value, or the
one imposed by the highest-priority matching `require` policy (`mode` on a
policy object). A namespace is a start-time property, so a policy that
changes `mode` affects the next placement, not a running allocation.

### Gated mode

With `mode = "gated"` the worker creates an empty network namespace for the
allocation before it starts — `/var/run/netns/legion-<allocation>`, loopback
up, no other interface, no route — and binds every datapath listener
*inside* it: the upstream loopback ports, the VIPs, the allocation's resolver
at `127.42.<b>.1:53`, and an **egress proxy** at `127.0.0.1:1080`. A
`container` joins the namespace by path (an OCI `network` namespace entry);
an `exec` task enters it with `setns` before exec, ahead of dropping
privileges. The worker dials out from the *host* namespace, so the listeners
are the only door.

Inside, the task gets:

| Env var | Value |
| --- | --- |
| `LEGION_NETWORK_MODE` | `gated` |
| `LEGION_EGRESS_PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` | `http://127.0.0.1:1080` |
| `NO_PROXY` | `localhost,127.0.0.0/8,svc.legion` |

The proxy speaks HTTP `CONNECT` (TLS and anything opaque, judged by host and
port), absolute-URI plain HTTP (judged by the request head too, forwarded in
origin form one request per connection), and SOCKS5 `CONNECT` (domain-name
targets judged by name). Every denial is a `403` with `Legion-Denied-By`, or
a SOCKS "connection not allowed", and a `net.denied` event. **A tool that
ignores the proxy variables has no route and fails** — that failure is the
enforcement, not a gap. Transparent capture (a TUN inside the namespace so
unmodified tools work) is a later phase.

Two fail-closed rules apply only to gated allocations: while the
allocation's policy has not arrived yet (`policy-pending`) and while its
[edge-boot](edge-boot.md) `boot` gate is shut (`gate`), every listener denies.
A gated allocation with *no* policy at all — nothing matched, nothing inline
— is told so explicitly and allows everything through the proxy, with
attribution. The `boot` gate is therefore a real boundary for gated
`container` and `exec` tasks, not the advisory one host networking gets, and
a gated task can be allowed the package registry before ratification by a
policy with only that host.

### Sandboxes

A `sandbox` task with `network.mode = "gated"` boots a microVM with **no
network interface at all**. Its only way out is a virtio-vsock device whose
host end is the node datapath: the worker binds the allocation's egress
proxy on the VMM's host socket for guest port 1080 and its resolver on the
one for port 53 (`<alloc dir>/.sandbox/vsock.sock_1080` and `_53`, the
naming both Firecracker and cloud-hypervisor use), before the guest boots.
Inside the guest, the generated init starts `legion guest-agent` — the
worker's own binary, copied into the rootfs as `/legion/legion` — before the
task's command. The agent relays `127.0.0.1:1080` to host vsock port 1080
and `127.0.0.1:53/udp` to host port 53 (DNS-over-TCP framing), so the same
`HTTP_PROXY` / `ALL_PROXY` / `LEGION_EGRESS_PROXY` variables and the same
`resolv.conf` a gated container gets work unchanged: every connection is
judged by the node, and the guest cannot bypass it because there is nothing
else to bypass to.

What is enforced and what is not:

- The guest is untrusted end to end: policy, handlers, identity and the
  graph all run on the host side of the vsock. The agent decides nothing.
- A gated sandbox cannot also have a TAP device (`config.network.mode = "tap"`
  is rejected at start), and needs a microVM backend — with `hypervisor =
  "dev"` (a container standing in for a VM) the start fails with that reason
  rather than running unprotected.
- The edge-boot `boot` gate is the same policy-withheld marker the vsock
  listeners check, so a provisional sandbox has no network until ratified.
- `legion net doctor` and the UI report the sandbox like any gated
  allocation: `capture_ready` means both host sockets are bound.

Honest status: the host side (VMM configuration, listeners, status) and the
agent are implemented and unit-tested over Unix sockets with the VMMs'
naming; an end-to-end boot with a real VMM is not part of this repository's
CI (no KVM there), which is why `auto` still means `host` for sandboxes. If
the copied binary cannot run on the image's libc, init says so on the
console and the guest has no network — fail-closed, and `net doctor` shows
the proxy never connecting. UDP inside the guest and transparent capture
remain the deferred phase.

Requirements and limits: Linux; the worker needs `CAP_SYS_ADMIN` (it already
does, for `exec` hardening's mount namespace and for running the OCI
runtime); IPv4 loopback only inside the namespace; no raw UDP, no ICMP; no
inbound except through the datapath's ingress listeners (below). macOS has
no namespaces: `gated` is rejected at start there with that reason.

## The datapath across worker restarts

Tasks survive a worker restart (the worker re-adopts their processes from
its state file). So does their network: the worker writes what the datapath
was last told — the desired set, the endpoint feed, the policy assignments
and their bodies — to `netd_state.json` beside `worker_state.json` after
every successful heartbeat that changed it, and a restarting worker replays
it *before* its first heartbeat. Namespaces persist on their own (bind
mounts under `/var/run/netns`); the replay re-binds every listener inside
and beside them: upstream ports, VIPs, resolvers, egress proxies, ingress
ports, plus identities and policy bodies, for the allocations that survived.
Fail-static on disk: a node that restarts while the manager is down comes
back with exactly the datapath it had.

Connections *in flight* across the restart drop — the listeners are new
sockets — and there is a gap of the process's own start time. The plan's
`legion netd` process split (listener hand-off with `SCM_RIGHTS`, zero
dropped connections) is deferred; this covers the failure that mattered:
a gated sandbox whose worker was upgraded no longer loses its network until
the manager is reachable again.

A gated allocation whose capture is not ready — a namespace or listener
missing, a vsock socket unbound — is reported **unhealthy** with
`net_capture_unavailable: <problems>` on the heartbeat, so a rollout does
not count it as up and `legion net doctor` names the cause.

## Ingress

Declare an `ingress` chain on a task — or impose one with a `require`
policy object — and the datapath fronts every port the task advertises:

```nickel
network = {
    ports = [{ name = "http", static = 8080 }],
    ingress = {
        protocol = "http",
        policy = "deny",
        rules = [
            { when = { identity = "auto:web:*" }, action = "allow" },     # <region>:<job>:<group>:<task>
            { when = { cidr = "127.0.0.0/8" }, action = "allow", reason = "health checks" },
        ],
        accessLog = true,
    },
},
```

- **The advertised port is the datapath's.** `LEGION_PORT_HTTP` as
  consumers see it — the port in the catalog and in `LEGION_UPSTREAM_*`
  feeds — is bound by the node's datapath on the host. The task is handed
  a **private twin** from the same dynamic range as *its* `LEGION_PORT_HTTP`
  (a container's port publish uses the twin too), so nothing in the task
  changes. The task cannot take the advertised port back: the bind fails
  because the address is taken. That is what makes ingress enforceable on
  every runtime and platform, not only gated Linux.
- **Every accepted connection is judged** against the ingress chain with
  the peer's address (`cidr`, `port`), and, from the mTLS phase, the peer's
  attributed identity (`identity`). In HTTP mode the request head is judged
  too, with the same request rules, `dlp`, structured `403`s and access log
  as egress. A denial closes the connection and records `net.denied` with
  `direction = ingress`.
- **Allowed connections are spliced** to the twin — inside the allocation's
  namespace when it is gated, on the host otherwise — or proxied per request
  in HTTP mode.
- **Gated allocations fail closed** on ingress exactly as on egress: while
  the policy is pending or the boot gate is shut, nothing is accepted.
- **Health checks go through the chain** — the checker dials the advertised
  port — so a chain that denies everything also denies the checker. Allow
  `cidr = "127.0.0.0/8"` for a node-local checker; `legion net doctor`
  warns when a chain would deny its own checks.

Limits: TCP only; the task sees the datapath as the peer (the attributed
peer is in the access log until the PROXY-protocol handoff lands with
external handlers); one listener per advertised port per allocation — the
cluster edge and per-tag routing on ingress are not this feature.

**Rollouts.** A task with a `service` block drains before it stops: it
leaves the catalog, keeps serving for one heartbeat interval (or
[`stop.drain`](graceful-stop.md)), and only then takes the stop ladder, so a
consumer dialing it through an upstream sees no `connection refused` while a
rollout replaces it.

## Service graph

Who calls whom, with RED — the reason to adopt a mesh, and the first thing
on the UI's Network page.

Every calling datapath — an upstream listener, the gated egress proxy, an
HTTP-mode request — records what it saw per (upstream, endpoint tag) since
the last heartbeat: connections or requests, errors (5xx, or no endpoint
reached), denials, latency, bytes. The samples ride the heartbeat
(`HeartbeatRequest.net_edges`, restored on a failed beat) and the manager
folds them into per-minute buckets per (caller job, caller task, service,
tag), keeping 15 minutes in memory. An edge appears within one heartbeat of
the first call.

```bash
legion service edges                 # the whole graph, last 5 minutes
legion service edges api --window 15m
```

```
CALLER          SERVICE   TAG     STATE                 RPS    ERRORS     DENIED  LATENCY avg/max  LAST SEEN
web/server      api       -       healthy (declared)    12.40  0 (0.0%)   0       18ms/240ms       3s ago
web/server      api       canary  some errors           1.30   4 (1.0%)   0       22ms/910ms       3s ago
agent-7/sandbox api       -       denials               0.00   0 (0.0%)   17      -                9s ago
batch/loader    db        -       declared, silent      0.00   0 (0.0%)   0       -                never
```

Each edge says whether the caller's latest job config **declares** the
upstream, whether the service's `allow` intentions **admit** the caller, and
what was **observed**: requests per second, error rate, denials, average and
max latency (HTTP mode), bytes. A declared upstream that never called shows
as `declared, silent`; a caller that calls without declaring (through the
gated proxy, say) shows without the `declared` mark; an edge into a service
whose `allow` list excludes the caller is `not allowed`. Tags come from the
endpoint the call was balanced to, so a `split` canary has its own row —
per-tag RED side by side.

The UI's **Network** page draws the graph (callers left, services right;
line width by request rate; dashed for silent, red for denials, errors above
5%, or a disallowed caller) over the same table, and a **Service** page
lists its callers. `ListServiceEdges` and the MCP tool
`legion_service_edges` return the same edges (`job:read`, like the catalog).

What is *not* on the graph: ingress-side attribution (edges are recorded by
the caller, so a client outside Legion dialing an advertised port is not an
edge — its denials are still events), traffic that bypasses the datapath in
`host` mode, and anything older than 15 minutes (the graph is in memory on
the manager; a failover starts it empty and the next heartbeats refill it).
Through the gated proxy, an external host is a node of the graph (service =
the destination host), bounded at 4,096 distinct edges per node between
heartbeats.

## CLI

```bash
legion net policy apply -f <file.ncl|file.json> [--dry-run]   # create or replace
legion net policy list                                          # merge order
legion net policy describe <name>
legion net policy delete <name>
legion net policy show <job> [--task <task>] [--pool <pool>]    # the effective policy
legion net policy test <job> --to <dest> [--method M --path P]  # dry-run a verdict
legion net policy test <job> --from <peer> [--identity <sub>]
legion net doctor <job> [--task <task>]                         # policy → mode → datapath → handlers → tls
legion net handler test <url|host:port>                         # probe an authz webhook or a proxy
legion net ca show | rotate                                     # the mesh CA
```

`net doctor` walks a task's network the way `service doctor` walks a
service: the resolved policy and where each chain came from; whether the
effective mode *enforces* it (`gated`) or relies on cooperation (`host`,
with the hint to gate it); then, per running allocation, what the node
reports — the namespace and every listener bound, resolver and egress proxy
serving, the policy hash the datapath is enforcing versus the one the
manager computes, `policy-pending` or a shut boot gate (fail-closed while
gated), and any problem the node has (an ingress listener that could not
bind, say). It stops at the first failure with a fix hint. Nodes report this
status on every heartbeat, and the same lines appear in the UI's task
Network section.

Every subcommand takes `--json`/`-j` and emits the standard envelope. `apply
--dry-run` validates and lists the running allocations whose effective policy
would change. `test` answers `allow`, `deny` (with the deciding rule and its
reason), or `defer` — a request-time rule would decide and no method/path
was given.

`show`, `test`, `doctor`, `handler test` and `ca show` are also MCP tools
(`legion_net_policy_show`, `legion_net_policy_test`, `legion_net_doctor`,
`legion_net_handler_test`, `legion_net_ca_show`) and `apply` and `ca rotate`
are mutating ones (`legion_net_policy_apply`, `legion_net_ca_rotate`), so an
agent can ask "is that host blocked?" instead of watching a connection hang.

## Observability

- **Metrics** (worker registry, bounded labels): `legion_net_conns_total{direction,protocol,verdict}`,
  `legion_net_denied_total{direction}`, `legion_net_bytes_total{direction}`,
  `legion_net_handler_total{handler,outcome}` (authz/proxy calls by
  allow|deny|proxy|error), `legion_net_handler_seconds` (webhook latency,
  cache misses only).
  Attribution is never a metric label — the registry caps label combinations
  and a per-user label would silently destroy the series.
- **Events**: `net.denied` (warn) with the destination, the deciding rule,
  the reason, the peer identity, and the task's `network.labels`;
  `net.policy_applied` and `net.policy_deleted` (info).
- **Access log** (`accessLog = true`, or a `log` handler): one NDJSON line
  per request in HTTP mode, at `<worker logs>/<allocation>/net-access.log`.
  L4 connections are counted in metrics, not logged per connection.
- **Datapath status**: every heartbeat carries, per allocation, the node's
  view of its datapath — mode, capture readiness, the enforced policy hash,
  bound upstream and ingress listeners, resolver and proxy state, the boot
  gate, and problems. `legion net policy show` returns it as
  `node_status_json`, `legion net doctor` interprets it, and the UI's task
  Network section shows it under **Datapath**.

## ACLs

A new resource, `net`: `read` lists policies, shows/tests a job's
effective policy (scoped by job name), and reads the mesh CA certificate;
`write` applies and deletes policies, scoped by `pools`, which is matched
against the policy's own `selector.nodePools` — so a pool's admin can set
posture for their pool without cluster-wide write. A cluster-wide policy (no
pools in its selector) and `ca rotate` need unscoped `net:write`.
`IssueNodeCertificate` is worker-plane: a node may only mint its own.

## Current scope and limitations

What ships now is the **policy and upstream layer on the connect datapath**;
the phases below are laid out in the plan and named here so nothing reads as
more than it is.

- **Rules and handlers are enforced on the datapath** — connect-time rules
  on every dial, request-time rules (path, method, header), `dlp`, the
  `http` traffic block, and the external `authz` and `proxy` hops on every
  path (upstream splices, HTTP mode, the gated egress proxy, ingress).
- **Egress is cooperative in `host` mode, unbypassable in `gated` mode.**
  In `host` mode policy applies to traffic that goes through an upstream and
  to names resolved through the allocation's resolver; a process that dials
  a raw address bypasses it. `mode = "gated"` (Linux, `container` and
  `exec`) closes that: the namespace has no route except Legion's listeners.
  On macOS egress stays cooperative and there are no VIPs, per-allocation
  resolvers, or gated namespaces. A gated `sandbox` is captured over vsock
  (implemented and unit-tested; not yet exercised on a real VMM in CI, so
  its `auto` still resolves to `host`).
- **Ingress is enforced everywhere it applies**: the datapath owns the
  advertised port and the task binds a private twin, on every runtime and
  platform. TCP only; the task sees the datapath as the peer until the
  PROXY-protocol handoff ships with external handlers.
- **TLS**: ingress chains terminate `server`/`mutual` TLS with your files
  or the mesh certificate; the mesh is node-to-node mTLS with attributed
  workload identity, never per-workload certificates. No kTLS fast path (a
  terminated connection is copied in user space), no revocation beyond the
  24-hour lifetime, and an ingress `proxy` hop cannot reach a *gated* task's
  private port.
- **No HTTP/2 or HTTP/3** (`protocol = "h2"` is rejected with that reason),
  no UDP, no WASM handlers, no TLS interception — the last by design:
  external upstreams are the body-visible route.
- **`selector.namespaces`** matches every job today (all jobs are in
  `default`); write it now and it starts scoping when namespaces land.
