# Network Handlers: Egress and Ingress Policy, Upstreams, and Middleware

Legion's node datapath is the loopback forwarder behind `connect.upstreams`.
It can decide, observe, and (when you ask) transform the traffic that crosses
a task's boundary. This page is the reference for the three concepts behind
that, and the surfaces you use them through:

- **Policy**: declarative rules that say what a task may reach and who may
  reach it. You write it inline on a task, or store it as a **policy object**
  that applies to every task a selector matches (a node pool, a runtime, a
  job glob, a label). Policy objects change live, without restarting
  anything.
- **Upstreams**: where a task's egress goes. That is a catalog service, or an
  **external destination** that Legion opens TLS to. The task speaks
  plaintext to a loopback port, so policy can see the request body without
  intercepting anyone's TLS.
- **Handlers**: code on the path, for when policy is not enough. A handler is
  a webhook that returns a verdict, or any proxy in any language.

This page describes what ships today and states what does not. See
[Current scope](#current-scope-and-limitations).

## Quick start

Deny everything except two registries, on the connect datapath, for one task:

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

Or apply it once for every sandbox on a pool, with nothing in the job:

```bash
legion net policy apply -f configs/policies/sandbox-egress.ncl
legion net policy show agent              # what the task actually gets, with sources
legion net policy test agent --to https://api.github.com/user   # would this be allowed?
```

## What the datapath does today

Every connection a task makes through an upstream (`LEGION_UPSTREAM_<KEY>`,
a loopback port the node owns) passes through the node's forwarder. The
forwarder runs in one of two modes.

**L4 (the default).** Before dialing, the forwarder evaluates the task's
**effective egress policy** using what is known at connect time: the
destination host (the service name, or an external upstream's host), the
port, and, for service upstreams, the endpoint about to be dialed. A denial
closes the connection without dialing, increments `legion_net_denied_total`,
and is reported on the next heartbeat as a `net.denied` event that names the
rule. An allowed connection is spliced through without parsing. The only
per-connection cost of a connect-time policy is one pass over the rules. No
bytes are inspected.

**HTTP mode.** An upstream is parsed as HTTP/1.1 when it declares HTTP (an
`http://`/`https://` target, an `http` block, or a `dlp` handler), or when
the effective egress policy is `protocol = "http"`. For each request:

1. The chain is evaluated with the request head (method, path, headers) in
   addition to the connect-time facts. A denial returns a `403`. Its body and
   its `Legion-Denied-By` / `Legion-Denied-Reason` headers name the rule, so
   a client (such as an agent in a sandbox) can read why it was denied
   instead of retrying.
2. `dlp` handlers buffer the body up to `maxBytes` and reject a match.
3. The upstream's `http` block applies traffic management:
   - a per-request `timeout`
   - `retries` on connect failures and 5xx, for idempotent methods only (by
     default), under a `budget` (default 20% of requests per minute)
   - a weighted `split` by service tag
   - `outlier` ejection of an endpoint after `consecutive5xx` failures, for
     `ejectFor`
4. Each request gets one upstream connection. TLS is opened for `https://`,
   and the `Host` header is rewritten to an external target's host.

`accessLog = true` on the chain (or a `log` handler) writes one NDJSON line
per request to `<worker logs>/<allocation>/net-access.log`. Each line has
the task's labels, the verdict, the deciding rule, status, bytes, and
duration.

HTTP mode supports HTTP/1.1 only, with no HTTP/2 and no WebSocket upgrades.
A task that needs those should use a `tcp://`/`tls://` upstream and an L4
policy.

## Policy

### The inline form

Set `network.egress` and `network.ingress` on a task. `allow` is a shorthand.
The full form has `rules`, `handlers`, and `tls`.

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

**Rules match on** these fields:

- `host`: a glob. `*` matches one or more labels at a label boundary, so
  `*.pypi.org` matches `files.pypi.org` but never `pypi.org`. A bare `*`
  matches everything.
- `port`
- `cidr`: the resolved destination, or the peer on ingress.
- `identity`: the verified peer's workload identity (ingress).
- With `protocol = "http"` only: `path` (`*` within a segment, `**` across
  segments), `method`, and `header` (name → value glob).

Every field present in a rule must match. The first matching rule wins, and
`policy` applies when nothing matches. A `deny` rule's `reason` is returned
to the workload and recorded on the event.

**`allow` entries** are `host`, `host:port`, `cidr`, `cidr:port`, an IP, or
`host/path` (only with `protocol = "http"`). A path in a `tcp` chain is
rejected at submit with a fix:

```
egress.allow[1] "api.github.com/repos/**" matches on a path, but
egress.protocol is "tcp". Set protocol = "http" to match on paths, or
drop the path to match the host only.
```

`protocol` is never inferred. If typing a slash silently turned a rule set
into L7, you could not review the policy just by reading it, and it would
cost about twice as much per byte.

### Policy objects

A stored policy is the same block plus a **selector** and an
**enforcement** mode. It lives in the cluster store (raft-replicated) and you
edit it with `legion net policy`. It applies to every matching task on the
task's next heartbeat, with no restart, rollout, or job edit.

```nickel
# configs/policies/sandbox-egress.ncl
{
    name = "sandbox-egress",
    "priority" = 100,
    selector = {
        nodePools = ["untrusted"],       # node.pool globs
        namespaces = ["default"],        # every job is in `default` (no user namespaces)
        runtimes = ["sandbox"],
        jobs = ["agent-*"],              # job id globs
        labels = { tier = "free" },      # task network.labels that must all match
    },
    enforcement = "require",             # require | default
    mode = "gated",                      # optional network.mode to impose
    egress = { protocol = "http", policy = "deny", allow = ["*.pypi.org", "api.github.com/repos/**"] },
}
```

`selector = {}` matches every task on the cluster. Values within a list are
ORed, and the fields are ANDed.

| `enforcement` | Applied when | A job author may |
| --- | --- | --- |
| `default` | the task declares no chain of its own for that direction | replace it entirely by declaring one |
| `require` | always, **ahead of** the task's own block | append rules and handlers and add denials, but never remove or widen |

**The merge is deterministic, and you can inspect it.** Every matching
`require` policy applies in `priority` order (ties broken by name). Then
exactly one of these applies: the task's own block, or the highest-priority
matching `default` policy. Nothing after a required `deny` fallthrough can
widen it. Two `default` policies that would tie for the same task (equal
priority, overlapping selectors) are rejected at `apply`, so the winner is
never decided at runtime. In the merged result, every rule and handler
carries a `source`: `policy/<name>#<index>` or `task#<index>`.

`legion net policy show <job>` prints that merged result. The node datapath
runs the same function on the same inputs.

### How policy reaches a node

The heartbeat reply carries one content hash per allocation. A node fetches
each body it has not seen once and caches it, so a thousand identical
sandboxes cost one fetch. Bodies are swapped only after a successful
heartbeat, and the last known good body is kept through a manager outage.
This is the same fail-static behavior as the endpoint feed. A cluster with no
stored policy and no task that declares one pays nothing extra per heartbeat.

## Upstreams

Each `connect.upstreams` entry is either a catalog **service** or an
**external destination**:

```nickel
connect = {
    upstreams = [
        { service = "db" },                                   # as before
        { name = "openai", to = "https://api.openai.com" },  # external
    ],
},
```

An external upstream has a `name` and a `to`. The task gets
`LEGION_UPSTREAM_<NAME>` (`127.0.0.1:<port>`), the same as for a service
upstream. The scheme of `to` decides two things:

| `to` | Legion originates TLS | HTTP framing |
| --- | --- | --- |
| `tcp://host:port` | no | no |
| `tls://host:port` | yes | no |
| `http://host[:port]` | no | yes |
| `https://host[:port]` | yes | yes |

With `https://`, the task speaks **plaintext HTTP to loopback**, and the
node's forwarder dials the destination over TLS (verified against the Mozilla
roots). This lets policy and handlers see an LLM API call's body **without
intercepting anyone's TLS**. Point the client at the upstream:

```sh
export OPENAI_BASE_URL="http://$LEGION_UPSTREAM_OPENAI/v1"
```

Legion never holds a certificate authority that can impersonate the public
internet. `to` must not include a path or query. The task chooses paths.

The `http` block on an upstream (`timeout`, `retries`, `split`, `outlier`)
turns on HTTP mode with traffic management. Per-upstream `handlers` run
there too. See [What the datapath does today](#what-the-datapath-does-today).

## Name-is-config: `<key>.svc.legion`

On Linux, every allocation that declares upstreams gets its own loopback /24
at placement (`127.42.<b>`, unique per node). All of 127/8 is loopback on
Linux with no configuration. In that block:

- **`.1` is the allocation's resolver**, exported as `LEGION_DNS`. It answers
  `<key>.svc.legion` (and bare `<key>`, through the `svc.legion` search
  domain) with that allocation's upstream VIP. SRV records carry the port.
  Other names are forwarded to the host's resolvers after the egress policy
  has checked the name. A host the policy would deny gets `NXDOMAIN` in one
  round trip and a `net.denied` event (`protocol: dns`), instead of a
  hanging connect. Names are not checked when the chain matches on CIDRs,
  because the rule needs the address.
- **`.2` onward are the upstream VIPs**, one per `connect.upstreams` entry in
  declaration order. Each listens on the upstream's **canonical port**,
  which is the first of these that applies: a pinned `localPort`, the port
  the service declared on its own task (`network.ports[].to`, or its static
  port), an external target's port, or the dynamic loopback port.
  `db.svc.legion:5432` works because the `db` service declared 5432.

Both addresses reach the same listener, so policy, HTTP mode, and endpoints
behave the same whichever one the task dials. The loopback port and
`LEGION_UPSTREAM_<KEY>` still work. The name is an addition.

| Env var | Value |
| --- | --- |
| `LEGION_DNS` | `127.42.<b>.1`, the allocation's resolver |
| `LEGION_UPSTREAM_HOST_<KEY>` | `<key>.svc.legion` |
| `LEGION_UPSTREAM_VIP_<KEY>` | `<vip>:<canonical port>` |

**Who gets the resolver automatically.** A `container` task's
`/etc/resolv.conf` is generated to point at it (`search svc.legion`,
`ndots:1`). An `exec` task gets `LEGION_DNS` and keeps the host's
`/etc/resolv.conf`, in gated mode too. Legion does not generate a
resolv.conf for `exec` tasks, so point your tool at `LEGION_DNS` to use the
allocation's resolver. A gated `sandbox` uses the guest
agent's relay as its resolver (`nameserver 127.0.0.1` in the guest's
`resolv.conf`, see "Sandboxes").

**macOS** configures only `127.0.0.1` on `lo0`, so no VIP block is assigned
there. Upstreams keep their loopback ports and env vars, and there is no
per-allocation resolver.

## Handlers

Handlers are code on the path. They only run when declared, either on a
chain (`egress.handlers`, `ingress.handlers`, on a task or a policy object)
or on one upstream (`connect.upstreams[].handlers`). They run in order after
the rules have allowed the flow, with the chain's handlers first, then the
upstream's.

| `handler` | What it does | Sees bytes |
| --- | --- | --- |
| `authz` | POSTs the flow's facts to `url` and reads back a verdict, cached per (hop, identity, destination[, method, path]) for `cache` (default `10s`, at most `5m`) | no |
| `proxy` | hands the flow to any proxy (a catalog `service`, its `node`-local endpoints, or a sidecar's `port` label in the same job) with a PROXY protocol v2 header (TCP) or `Legion-*` headers (HTTP) | yes |
| `log` | access log for this hop | as declared |
| `dlp` | rejects HTTP bodies matching `config.patterns` up to `config.maxBytes` (needs `protocol = "http"`) | yes |

Every hop has a `timeout` (default `2s`) and an `onError` (default `deny`).
You cannot declare a handler without a timeout. If a hop cannot be reached
(a webhook that times out, a proxy with no endpoint), the chain fails
**closed** unless the hop sets `onError = "allow"`.

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

Most custom logic fits a webhook that answers "may this workload talk to
that destination?" Legion asks it once per new (identity, destination) pair
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

`method`, `path`, and `headers` are present in HTTP mode only.
`authorization`, `cookie`, and `proxy-authorization` are never forwarded.
`peerIdentity` is the verified client on ingress.

**Reply** (`200`): `{ "verdict": "allow" | "deny", "reason": "...", "ttl": "1m" }`.
Anything else is a handler error and follows `onError`. That includes a
non-2xx status, a body that is not JSON, a verdict other than
`allow`/`deny`, and a timeout. A `deny` works like any other policy denial.
The connection closes (or the request gets a `403` that names the hop in
`Legion-Denied-By`), a `net.denied` event carries the reason, and
`legion net policy show` names the hop.

**Where it runs.** On L4 paths (an upstream splice, the gated egress proxy's
`CONNECT`/SOCKS, an ingress splice), the webhook is called at connect time
with the connect-time facts. In HTTP mode it is called per request with the
request head: once per new (identity, destination, method, path), then from
the cache.

### `proxy` — bring your own proxy

A `proxy` hop takes over the flow. Legion dials the proxy, hands it the
connection along with everything it needs to decide or filter, and then
splices:

- **TCP** (an L4 upstream, `CONNECT`, SOCKS5, an ingress splice): the
  connection starts with a **PROXY protocol v2** header. It carries the
  source and destination addresses, plus these Legion TLVs: `0xE0`
  identity, `0xE1` the original destination as `host:port` (or the task's
  private port on ingress), `0xE2` allocation id, `0xE3` labels
  (`k=v;...`), `0xE4` direction, `0xE5` job, `0xE6` task. HAProxy, Envoy,
  nginx, and Caddy read the addresses and ignore TLVs they do not know. Set
  `proxyProtocol = false` to leave out the header for a proxy that cannot
  accept one.
- **HTTP** (an HTTP-mode upstream, an absolute-URI request through the
  gated proxy): the request reaches the proxy in **absolute form**
  (`GET http://api.example/x HTTP/1.1`) with headers `Legion-Identity`,
  `Legion-Allocation`, `Legion-Job`, `Legion-Task`, `Legion-Labels`,
  `Legion-Direction`, `Legion-Destination`, and `Legion-Peer-Identity` on
  ingress. The proxy answers for the origin. A `4xx` from the proxy is the
  denial.

Targets:

- `service = "x"`: any healthy endpoint of catalog service `x`, round-robin.
- `node = "x"`: only endpoints of `x` on the same node, such as a per-node
  filter run as a system job.
- `port = "filter"`: a sidecar task in the same job with a port of that
  label, on this node.

The manager sends the endpoints on the heartbeat, as it does for an
upstream. A target with no endpoint is a handler error (`onError`).

An ingress `proxy` hop hands the inbound connection to the proxy, which
reaches the task on the private port named in the header. This works for a
task in host mode, where the proxy can dial the port. A gated task's private
port is inside its namespace, which an external proxy cannot enter.

### Health, and `legion net handler test`

Every hop the datapath has run is reported on the heartbeat
(`NetAllocStatus.handlers`: source, kind, target, healthy, calls, errors,
last error). `legion net doctor` shows it, and so does the **Datapath** area
of the UI's task Network section. Test a handler before you wire it in:

```bash
legion net handler test http://policy.internal:8080/check   # authz: sample facts → verdict?
legion net handler test 10.0.0.9:9000                       # proxy: accepts a PROXY v2 header?
```

The probe sends one synthetic request the way the datapath would. It prints
each check (`reachable`, `status_2xx`, `verdict`, `ttl`, or
`proxy_protocol`) with a fix, and exits non-zero when a check fails. It is
also available as an MCP tool (`legion_net_handler_test`).

## mTLS

Legion terminates and originates TLS, so a handler never sees it. You choose
one of two kinds of certificate material per chain:

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
| `mode` | `server` presents a certificate. `mutual` also verifies the client's. Egress: `passthrough` (default, opaque) or `originate` (external upstreams, with TLS from the node and plaintext from the task). |
| `certFrom = "legion"` | the node's mesh certificate (below). Omit it and give `certPath`/`keyPath` for bring-your-own files on the node (or staged artifacts). |
| `clientCa` | `"legion"` (the mesh CA) or a CA bundle path. Required for `mutual`. |
| `requireIdentity` | identity globs. A verified peer outside them is refused with `Legion-Denied-By: tls/requireIdentity` and a `net.denied` event. Empty = any verified peer. |
| `caPath` | egress `originate`: verify the destination against this bundle instead of the system roots. |

**Bring-your-own PKI.** With `certPath`/`keyPath` (and `clientCa` for
`mutual`), the advertised port terminates TLS with your certificate. A
`mutual` peer's identity is its certificate's first URI SAN, or else its
first DNS SAN, or else its CN. `identity` rules and `requireIdentity` match
against that string.

**The mesh (`certFrom = "legion"`).** The manager keeps one cluster **mesh
CA**, created on first use and replicated with the control lane. It issues
each worker node a short-lived **node certificate** over the authenticated
worker channel (`IssueNodeCertificate`). The certificate has the SANs
`<node>.node.legion` and `legion://node/<node id>`, is valid for 24 hours,
and the worker refreshes it after 8. Nothing is written to disk on the node.
Then:

- A task whose ingress chain sets `mode = "mutual", certFrom = "legion"` has
  its advertised port terminate **node-to-node mTLS**. The catalog marks the
  endpoint `mesh_tls`. Each consumer's datapath dials it with its own node
  certificate, verifies the peer node, and names the consuming **workload**
  (`<region>:<job>:<group>:<task>`) in a PROXY v2 TLV inside the session.
  The receiving node trusts that attribution because the peer proved it is a
  node. It passes the identity to `identity` rules, `requireIdentity`, and
  `authz`.
- A plain client that dials the port directly is refused at the handshake.
  A non-Legion client that must reach the port has two options: present a
  certificate from the mesh CA (`legion net ca show` prints the CA, but
  Legion does not issue certificates for outside clients), or go through a
  Legion task.
- Workloads never hold certificates. The node attributes identity, which
  keeps a per-user sandbox's identity out of the sandbox.

`legion net ca show` prints the CA certificate (never the key).
`legion net ca rotate` replaces it. Every node re-issues its certificate on
its next heartbeat, and until then, peers on different CAs refuse each other.
They never fall back to plaintext.

**Doctor.** `legion net doctor` reports `tls` as a layer. It flags BYO files
that do not parse, and a mesh certificate the node has not received yet.
Until it arrives, mesh peers are refused, and the allocation's `problems`
say so.

Limits: there is no kTLS/`splice` fast path, so a terminated mesh connection
is copied in user space. TLS 1.2 and 1.3 are supported. There is no
certificate revocation. The 24-hour lifetime takes its place.

## `network.mode`

| `mode` | Meaning | Default for |
| --- | --- | --- |
| `host` | host networking. Policy is cooperative (proxy env vars, upstreams, the resolver) | `container`, `exec`, `sandbox` |
| `gated` | the workload's network namespace holds loopback and the datapath only. It cannot be bypassed on Linux for `container` and `exec`. For a `sandbox`, it is a microVM with no NIC whose only way out is a vsock to the datapath (see "Sandboxes") | — |
| `auto` | `host` for every runtime | — |
| `bridge`, `cni/<name>` | reserved for the datapath plugin, rejected at submit | — |

The effective mode is resolved **at placement**. It is the task's own value,
or the one imposed by the highest-priority matching `require` policy (`mode`
on a policy object). A namespace is set when the task starts, so a policy
that changes `mode` affects the next placement, not a running allocation.

### Gated mode

With `mode = "gated"`, the worker creates an empty network namespace for the
allocation before it starts: `/var/run/netns/legion-<allocation>`, with
loopback up, no other interface, and no route. It binds every datapath
listener inside that namespace: the upstream loopback ports, the VIPs, the
allocation's resolver at `127.42.<b>.1:53`, and an **egress proxy** at
`127.0.0.1:1080`. A `container` joins the namespace by path (an OCI
`network` namespace entry). An `exec` task enters it with `setns` before
exec, before dropping privileges. The worker dials out from the host
namespace, so the listeners are the only way out.

Inside, the task gets:

| Env var | Value |
| --- | --- |
| `LEGION_NETWORK_MODE` | `gated` |
| `LEGION_EGRESS_PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` | `http://127.0.0.1:1080` |
| `NO_PROXY` | `localhost,127.0.0.0/8,svc.legion` |

The proxy supports three protocols:

- HTTP `CONNECT`, for TLS and other opaque traffic, checked by host and port.
- Absolute-URI plain HTTP, also checked by the request head and forwarded in
  origin form, one request per connection.
- SOCKS5 `CONNECT`, with domain-name targets checked by name.

Every denial is a `403` with `Legion-Denied-By` (or a SOCKS "connection not
allowed") and a `net.denied` event. **A tool that ignores the proxy
variables has no route and fails.** That failure is how the policy is
enforced. Transparent capture (a TUN inside the namespace so unmodified tools
work) is not implemented.

Two fail-closed rules apply only to gated allocations. Every listener denies
while the allocation's policy has not arrived (`policy-pending`), and while
its [edge-boot](edge-boot.md) `boot` gate is shut (`gate`). The gate check
runs before the policy, so a shut gate denies every destination, even ones
the policy allows. A gated allocation with no policy at all (nothing matched
and nothing inline) is told so explicitly, and it allows everything through
the proxy, with attribution. So for gated `container` and `exec` tasks, the
`boot` gate is enforced, unlike the advisory gate in host networking.

### Sandboxes

A `sandbox` task with `network.mode = "gated"` boots a microVM with **no
network interface at all**. Its only way out is a virtio-vsock device whose
host end is the node datapath. Before the guest boots, the worker binds the
allocation's egress proxy on the VMM's host socket for guest port 1080, and
its resolver on the socket for port 53. The sockets are
`<alloc dir>/.sandbox/vsock.sock_1080` and `_53`, the naming both
Firecracker and cloud-hypervisor use.

Inside the guest, the generated init starts `legion guest-agent` before the
task's command. The agent is the worker's own binary, copied into the rootfs
as `/legion/legion`. It relays `127.0.0.1:1080` to host vsock port 1080 and
`127.0.0.1:53/udp` to host port 53 (using DNS-over-TCP framing). So the same
`HTTP_PROXY` / `ALL_PROXY` / `LEGION_EGRESS_PROXY` variables and the same
`resolv.conf` a gated container gets work unchanged. The node checks every
connection, and the guest cannot bypass it because there is no other way
out.

What is and is not enforced:

- The guest is untrusted throughout. Policy, handlers, identity, and the
  service graph all run on the host side of the vsock. The agent decides
  nothing.
- A gated sandbox cannot also have a TAP device
  (`config.network.mode = "tap"` is rejected at start). It also needs a
  microVM backend. With `hypervisor = "dev"` (a container standing in for a
  VM), the start fails with that reason instead of running unprotected.
- The edge-boot `boot` gate uses the same policy-withheld marker the vsock
  listeners check, so a provisional sandbox has no network until it is
  ratified.
- `legion net doctor` and the UI report the sandbox like any gated
  allocation. `capture_ready` means both host sockets are bound.

Current status: the host side (VMM configuration, listeners, status) and the
agent are implemented and unit-tested over Unix sockets with the VMMs'
naming. This repository's CI has no KVM, so it does not run an end-to-end
boot with a real VMM. `auto` resolves to `host` for sandboxes, as it does
for every runtime.
If the copied binary cannot run on the image's libc, init reports it on the
console and the guest has no network. This fails closed, and `net doctor`
shows the proxy never connecting. UDP inside the guest and transparent
capture are not implemented.

Requirements and limits:

- Linux only. macOS has no network namespaces, so `gated` is rejected at
  start there with that reason.
- The worker needs `CAP_SYS_ADMIN`. It already does, for `exec` hardening's
  mount namespace and for running the OCI runtime.
- IPv4 loopback only inside the namespace.
- No raw UDP and no ICMP.
- No inbound traffic except through the datapath's ingress listeners (below).

## The datapath across worker restarts

Tasks survive a worker restart, because the worker re-adopts their processes
from its state file. Their network survives too. After every successful
heartbeat that changed it, the worker writes what the datapath was last told
to `netd_state.json`, next to `worker_state.json`. That includes the desired
set, the endpoint feed, the policy assignments, and their bodies. A
restarting worker replays this file before its first heartbeat.

Namespaces persist on their own (bind mounts under `/var/run/netns`). For
the allocations that survived, the replay re-binds every listener inside and
next to them: upstream ports, VIPs, resolvers, egress proxies, and ingress
ports, plus identities and policy bodies. Because this state is on disk, a
node that restarts while the manager is down comes back with the same
datapath it had.

Connections in flight during the restart drop, because the listeners are new
sockets. There is also a gap while the process starts. A separate
`legion netd` process with listener hand-off (`SCM_RIGHTS`) and zero dropped
connections is not implemented. This replay fixes the main problem: a gated
sandbox whose worker is upgraded no longer loses its network until the
manager is reachable again.

A gated allocation whose capture is not ready (a missing namespace or
listener, or an unbound vsock socket) is reported **unhealthy** with
`net_capture_unavailable: <problems>` on the heartbeat. A rollout does not
count it as up, and `legion net doctor` names the cause.

## Ingress

Declare an `ingress` chain on a task, or impose one with a `require` policy
object, and the datapath fronts every port the task advertises:

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

- **The datapath owns the advertised port.** The node's datapath binds
  `LEGION_PORT_HTTP` as consumers see it (the port in the catalog and in
  `LEGION_UPSTREAM_*` feeds) on the host. The task gets a **private twin**
  port from the same dynamic range as its own `LEGION_PORT_HTTP` (a
  container's port publish uses the twin too), so nothing in the task
  changes. The task cannot take the advertised port back, because the bind
  fails with the address already in use. This makes ingress enforceable on
  every runtime and platform, not only on gated Linux.
- **Every accepted connection is checked** against the ingress chain using
  the peer's address (`cidr`, `port`) and, with mesh mTLS, the peer's
  attributed identity (`identity`). In HTTP mode, the request head is
  checked too, with the same request rules, `dlp`, structured `403`s, and
  access log as egress. A denial closes the connection and records
  `net.denied` with `direction = ingress`.
- **Allowed connections are spliced** to the twin port (inside the
  allocation's namespace when it is gated, on the host otherwise), or
  proxied per request in HTTP mode.
- **Gated allocations fail closed** on ingress the same way as on egress.
  While the policy is pending or the boot gate is shut, nothing is accepted.
- **Health checks go through the chain**, because the checker dials the
  advertised port. A chain that denies everything also denies the checker.
  Allow `cidr = "127.0.0.0/8"` for a node-local checker. `legion net doctor`
  warns when a chain would deny its own checks.

Limits: TCP only. The task sees the datapath as the peer. Legion does not
pass the original peer to the task (no PROXY header and no forwarding
headers). The original peer appears in the access log and in `net.denied`
events, and an ingress `proxy` hop receives it in the PROXY v2 header. There
is one listener per advertised port per allocation. The cluster edge and per-tag routing on ingress are not part of
this feature.

**Rollouts.** A task with a `service` block drains before it stops. It
leaves the catalog, keeps serving for one heartbeat interval (or
[`stop.drain`](graceful-stop.md)), and only then begins its normal stop. A
consumer dialing it through an upstream sees no `connection refused` while a
rollout replaces it.

## Service graph

The service graph shows who calls whom, with RED metrics (rate, errors,
duration). It is a main reason to adopt a mesh, and the first thing on the
UI's Network page.

Every calling datapath (an upstream listener, the gated egress proxy, an
HTTP-mode request) records what it saw per (upstream, endpoint tag) since
the last heartbeat: connections or requests, errors (5xx, or no endpoint
reached), denials, latency, and bytes. The samples ride on the heartbeat
(`HeartbeatRequest.net_edges`, restored on a failed heartbeat). The manager
rolls them into per-minute buckets per (caller job, caller task, service,
tag) and keeps 15 minutes in memory. An edge appears within one heartbeat of
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

Each edge shows whether the caller's latest job config **declares** the
upstream, whether the service's `allow` intentions **admit** the caller, and
what was **observed**: requests per second, error rate, denials, average and
max latency (HTTP mode), and bytes.

- A declared upstream that has never been called shows as
  `declared, silent`.
- A caller that calls without declaring (through the gated proxy, for
  example) shows without the `declared` mark.
- An edge into a service whose `allow` list excludes the caller shows as
  `not allowed`.

Tags come from the endpoint the call was balanced to, so a `split` canary
gets its own row, with per-tag RED side by side.

The UI's **Network** page draws the graph above the same table. Callers are
on the left and services on the right. Line width shows request rate. Lines
are dashed for silent edges and red for denials, errors above 5%, or a
disallowed caller. A **Service** page lists that service's callers.
`ListServiceEdges` and the MCP tool `legion_service_edges` return the same
edges (`job:read`, like the catalog).

The graph does not include:

- Ingress-side attribution. The caller records edges, so a client outside
  Legion dialing an advertised port is not an edge. Its denials are still
  events.
- Traffic that bypasses the datapath in `host` mode.
- Anything older than 15 minutes. The graph lives in memory on the manager.
  A failover starts it empty, and the next heartbeats refill it.

Through the gated proxy, an external host appears as a node of the graph
(the service is the destination host). This is capped at 4,096 distinct
edges per node between heartbeats.

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

`net doctor` walks through a task's network the way `service doctor` walks
through a service. It shows:

- the resolved policy and where each chain came from
- whether the effective mode enforces it (`gated`) or relies on cooperation
  (`host`, with a hint to gate it)
- for each running allocation, what the node reports: the namespace and
  every listener bound, whether the resolver and egress proxy are serving,
  the policy hash the datapath enforces compared to the one the manager
  computes, `policy-pending` or a shut boot gate (fail-closed while gated),
  and any problem the node has (for example, an ingress listener that could
  not bind)

It stops at the first failure and gives a hint for fixing it. Nodes report
this status on every heartbeat, and the same lines appear in the UI's task
Network section.

Every subcommand takes `--json`/`-j` and emits the standard envelope.
`apply --dry-run` validates the policy and lists the running allocations
whose effective policy would change. `test` answers `allow`, `deny` (with
the deciding rule and its reason), or `defer`, which means a request-time
rule would decide and no method or path was given.

`show`, `test`, `doctor`, `handler test`, and `ca show` are also MCP tools
(`legion_net_policy_show`, `legion_net_policy_test`, `legion_net_doctor`,
`legion_net_handler_test`, `legion_net_ca_show`). `apply` and `ca rotate`
are mutating MCP tools (`legion_net_policy_apply`, `legion_net_ca_rotate`).
An agent can ask "is that host blocked?" instead of watching a connection
hang.

## Observability

- **Metrics** (worker registry, bounded labels): `legion_net_conns_total{direction,protocol,verdict}`,
  `legion_net_denied_total{direction}`, `legion_net_bytes_total{direction}`,
  `legion_net_handler_total{handler,outcome}` (authz/proxy calls by
  allow|deny|proxy|error), `legion_net_handler_seconds` (webhook latency,
  cache misses only).
  Attribution is never a metric label. The registry caps label
  combinations, and a per-user label would silently break the series.
- **Events**: `net.denied` (warn) with the destination, the deciding rule,
  the reason, the peer identity, and the task's `network.labels`.
  `net.policy_applied` and `net.policy_deleted` (info).
- **Access log** (`accessLog = true`, or a `log` handler): one NDJSON line
  per request in HTTP mode, at `<worker logs>/<allocation>/net-access.log`.
  L4 connections are counted in metrics, not logged one by one.
- **Datapath status**: every heartbeat carries the node's view of each
  allocation's datapath: mode, capture readiness, the enforced policy hash,
  bound upstream and ingress listeners, resolver and proxy state, the boot
  gate, and problems. `legion net policy show` returns it as
  `node_status_json`, `legion net doctor` interprets it, and the UI's task
  Network section shows it under **Datapath**.

## ACLs

The `net` resource controls access:

- `read` lists policies, shows and tests a job's effective policy (scoped by
  job name), and reads the mesh CA certificate.
- `write` applies and deletes policies. It is scoped by `pools`, which is
  matched against the policy's own `selector.nodePools`, so a pool's admin
  can set policy for their pool without cluster-wide write access.

A cluster-wide policy (no pools in its selector) and `ca rotate` need
unscoped `net:write`. `IssueNodeCertificate` is worker-plane, and a node may
only mint its own certificate.

## Current scope and limitations

What ships now is the **policy and upstream layer on the connect datapath**.
This section lists what that covers and what it does not.

- **Rules and handlers are enforced on the datapath.** That includes
  connect-time rules on every dial, request-time rules (path, method,
  header), `dlp`, the `http` traffic block, and the external `authz` and
  `proxy` hops on every path (upstream splices, HTTP mode, the gated egress
  proxy, ingress).
- **Egress is cooperative in `host` mode and cannot be bypassed in `gated`
  mode.** In `host` mode, policy applies to traffic that goes through an
  upstream and to names resolved through the allocation's resolver. A
  process that dials a raw address bypasses it. `mode = "gated"` (Linux,
  `container` and `exec`) closes that gap, because the namespace has no
  route except Legion's listeners. On macOS, egress stays cooperative, and
  there are no VIPs, per-allocation resolvers, or gated namespaces. A gated
  `sandbox` is captured over vsock. That path is implemented and
  unit-tested but not exercised on a real VMM in CI. `auto` resolves to
  `host` for every runtime, so set `gated` explicitly.
- **Ingress is enforced everywhere it applies.** The datapath owns the
  advertised port and the task binds a private twin, on every runtime and
  platform. TCP only. The task sees the datapath as the peer, not the
  original client.
- **TLS**: ingress chains terminate `server`/`mutual` TLS with your files or
  the mesh certificate. The mesh is node-to-node mTLS with attributed
  workload identity, never per-workload certificates. There is no kTLS fast
  path (a terminated connection is copied in user space), no revocation
  beyond the 24-hour lifetime, and an ingress `proxy` hop cannot reach a
  gated task's private port.
- **No HTTP/2 or HTTP/3** (`protocol = "h2"` is rejected with that reason),
  no UDP, no WASM handlers, and no TLS interception. Legion will not add TLS
  interception. Use external upstreams when policy needs to see the body.
- **Legion has no job namespaces.** Every job is in `default`, so
  `selector.namespaces` either matches every job or none.
