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
one of three kinds of certificate material per chain: the mesh certificate,
a certificate held in KV, or files on the node.

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
| `certFrom = { kv = "<path>" }` | a certificate held in KV, swapped live when the key changes. Ingress only. See [Certificates from KV](#certificates-from-kv). |
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

The mesh CA is the cluster CA, the same one behind the worker plane
([Auth & ACLs](auth-and-acls.md#the-worker-plane)). A node's mesh
certificate is its node certificate: the worker generates the key and the
manager signs a CSR.

`legion net ca show` prints the CA trust bundle (never a key), its pin, and
where a rotation stands. `legion net ca rotate` rotates it in two steps that
break no mesh connection: the new CA, cross-signed by the old one, signs at
once, and the old one stays in every trust bundle for one certificate
lifetime (24 hours). Every node re-issues on its next heartbeat. Peers never
fall back to plaintext ([Rotating the cluster
CA](auth-and-acls.md#rotating-the-cluster-ca)).

### Certificates from KV

A public certificate (from Let's Encrypt or your own CA) can live in KV
instead of on node disks. The node serves it on the advertised port and swaps
it when the key changes, so renewal is a single `kv put`:

```nickel
network = {
    ports = [{ name = "https", static = 443 }],
    ingress = {
        tls = { mode = "server", certFrom = { kv = "certs/app.example.com" } },
    },
},
```

```bash
legion kv put certs/app.example.com "$(cat fullchain.pem privkey.pem)"
```

- **One value, one bundle.** The KV value is PEM: the certificate chain, leaf
  first, then the private key. Keeping both in one key means a reader never
  pairs a new certificate with an old key.
- **The task never holds the key.** The task speaks plain HTTP (or any TCP)
  on its private port. The node terminates TLS.
- **Swaps are live.** Each heartbeat, a node with KV-backed chains asks the
  manager for certificates newer than the version it holds
  (`GetIngressCertificates`). New connections get the new certificate, and
  connections already open keep theirs. Nothing restarts.
- **Fail-static.** The manager checks a bundle before sending it: it must hold
  a certificate and a private key, and the key must match the leaf. A bad
  value, a deleted key, or an unreachable manager leaves the node serving the
  last good certificate. The allocation reports the problem: `tls_error`
  names the key, and `problems` reads
  `ingress TLS: serving the previous certificate; kv certs/app.example.com holds no private key`.
  An allocation that has never had a good certificate is not ready, and its
  handshakes fail.
- **Access.** Naming a key in `certFrom` hands its value to the datapath, so
  it needs the right to read it. `RunJob` (and template runs) and
  `UpsertNetPolicy` require `kv:read` on the path in addition to their own
  permissions. Without it the submit fails with `PermissionDenied`, naming
  the key. A node can only fetch the key that an allocation placed on it
  declares. It names allocations, and the manager derives each path itself.
- **Where it works.** Inline ingress chains and policy objects. `mode` may be
  `server` or `mutual`, with `clientCa = "legion"` or a CA bundle path for
  `mutual`. Not on egress.
- **In memory only.** The node never writes the material to disk, including
  its `netd_state.json`. After a worker restart it fetches the certificate
  again on its first heartbeat.

To renew automatically, run a scheduled batch job that puts the new bundle.
[`configs/jobs/cert-renewer.ncl`](../configs/jobs/cert-renewer.ncl) does it
with lego and DNS-01, and
[`configs/jobs/tls-edge.ncl`](../configs/jobs/tls-edge.ncl) serves the result.
For a task that terminates TLS itself, such as nginx, deliver the same key as
a file with `onChange = "update"` instead
([Live file updates](kv-store.md#live-file-updates)).

Limits: a renewal reaches the node on its next heartbeat, so expect up to
one heartbeat interval of delay. There is no `clientCa = { kv }` yet: a
client CA bundle comes from the mesh or a path. Legion does not run an ACME
client itself.

**Doctor.** `legion net doctor` reports `tls` as a layer. It flags BYO files
that do not parse, and a mesh certificate the node has not received yet.
Until it arrives, mesh peers are refused, and the allocation's `problems`
say so. For a KV-backed chain, it names the key and shows the
`legion kv put` that fixes it.

Limits: there is no kTLS/`splice` fast path, so a terminated mesh connection
is copied in user space. TLS 1.2 and 1.3 are supported. There is no
certificate revocation. The 24-hour lifetime takes its place.

## `network.mode`

| `mode` | Meaning | Default for |
| --- | --- | --- |
| `host` | host networking. Policy is cooperative (proxy env vars, upstreams, the resolver) | `container`, `exec`, `sandbox` |
| `gated` | the workload's network namespace holds loopback and the datapath only, so a `container` or non-root `exec` task can't open a socket around it. Every destination the egress proxy dials is judged by the [host guard](#the-host-guard) and then by policy, by the address a name resolves to. A root `exec` task can leave the namespace. For a `sandbox`, it is a microVM with no NIC whose only way out is a vsock to the datapath (see "Sandboxes") | — |
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
| `LEGION_EGRESS_PROXY`, `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and lower-case `http_proxy`, `https_proxy`, `all_proxy` | `http://127.0.0.1:1080` |
| `NO_PROXY`, `no_proxy` | `localhost,127.0.0.0/8,svc.legion` |
| `NODE_USE_ENV_PROXY` | `1` |

Both cases are set because tools disagree: curl ignores upper-case
`HTTP_PROXY` for plain HTTP, and apt reads only lower case.
`NODE_USE_ENV_PROXY=1` makes Node's built-in `fetch` and `http` honor the
variables on Node 22.21 and 24.5 or later. Older Node ignores it, and so has
no route out.

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

Three fail-closed rules apply only to gated allocations. Every listener
denies while the allocation's policy has not arrived (`policy-pending`), and
while its [edge-boot](edge-boot.md) `boot` gate is shut (`gate`). The gate
check runs before the policy, so a shut gate denies every destination, even
ones the policy allows. So for gated `container` and `exec` tasks, the `boot`
gate is enforced, unlike the advisory gate in host networking. And a gated
allocation that nothing allows is denied, below.

### Default deny

**Gated means nothing leaves unless a rule says so.** A gated allocation
whose egress no task block and no policy object covers (nothing matched and
nothing inline) denies every destination, on every runtime. The verdict
names its own source, `gated-default`, so the `403`, the `net.denied` event,
and `legion net policy test` all say why:

```console
$ curl -sS https://example.com          # inside the gated task
curl: (56) CONNECT tunnel failed, response 403

$ legion net policy test agents --to https://example.com
✗ deny https://example.com: denied by gated-default — task workspace is gated and nothing allows example.com
  Hint: add "example.com" to network.egress.allow, or apply a policy object; network.egress = { policy = "allow" } allows everything
```

A plain-HTTP request gets the JSON `403` body with
`Legion-Denied-By: gated-default`. The allocation's resolver answers a name
nothing allows with `NXDOMAIN` in one round trip and records the same
denial, while its own `*.svc.legion` names still resolve. Declared
`connect.upstreams` are an explicit allowance and are not affected.
`legion net doctor` warns on the policy layer when a gated task has nothing
covering its egress.

Opening it is one line in the task:

```nickel
network = { mode = "gated", egress = { allow = ["pypi.org", "files.pythonhosted.org"] } },
```

To observe everything and block nothing, which is what a gated allocation
with no policy did before, write it:

```nickel
network = { mode = "gated", egress = { policy = "allow", accessLog = true } },
```

For agent sandboxes, `configs/policies/agent-sandbox-egress.ncl` is a
ready-made `default` policy for `runtimes = ["sandbox"]` that opens the
common read paths: PyPI's index and files, npm's registry, crates.io, Go's
module proxy, and GitHub's clone and download hosts. Read it before you
apply it. Behind HTTPS `CONNECT` the proxy sees a host and a port, not a
method, so a registry it opens for downloads is open for uploads too (npm's
registry takes `npm publish`, `github.com` takes `git push`), and each line
says so. It never applies unless you apply it, and it never lists a model
API: which one your platform calls is yours to add.

```console
legion net policy apply -f configs/policies/agent-sandbox-egress.ncl
```

### Sandboxes

A `sandbox` task with `network.mode = "gated"` boots a microVM with **no
network interface at all**. Its only way out is a virtio-vsock device whose
host end is the node datapath. Before the guest boots, the worker binds the
allocation's egress proxy on the VMM's host socket for guest port 1080, and
its resolver on the socket for port 53. The sockets are
`<alloc dir>/.sandbox/vsock.sock_1080` and `_53`, the naming both
Firecracker and cloud-hypervisor use. Every gated sandbox gets the resolver,
whether or not it declares upstreams, so names resolve in the guest from the
first boot.

Inside the guest, the generated init starts `legion guest-agent` before the
task's command. It relays `127.0.0.1:1080` to host vsock port 1080 and
`127.0.0.1:53/udp` to host port 53 (using DNS-over-TCP framing). So the same
proxy variables and the same `resolv.conf` a gated container gets work
unchanged. The node checks every connection (the [host guard](#the-host-guard)
first, then policy), and the guest has no other way out.

**The guest agent binary.** The worker copies `legion-guest`, a statically
linked build of the agent and the resume handler, into the rootfs as
`/legion/legion`, so it runs on any image, `alpine:latest` and busybox
included. Install it next to the worker binary (or point
`LEGION_GUEST_BINARY` at it in the worker's environment). Without it, the
worker copies itself and logs a warning at start. That fallback needs a
compatible glibc in the image: on a musl image the agent cannot run, init
says so on the console, and the guest has no network, which fails closed.

What is and is not enforced:

- The guest is untrusted throughout. Policy, handlers, identity, and the
  service graph all run on the host side of the vsock. The agent decides
  nothing.
- A gated guest that no policy covers denies everything
  ([Default deny](#default-deny)).
- A gated sandbox cannot also have a `config.network` (a TAP, or `none`):
  submit refuses the pair, `task "workspace" sets network.mode = "gated" and
  config.network = { mode = "tap" }. A gated guest has no NIC`, with a hint
  to drop one of them. It also needs a microVM backend. With `hypervisor =
  "dev"` (a container standing in for a VM), the start fails with that
  reason instead of running unprotected.
- Ingress into a gated sandbox is not supported yet: the guest has no NIC,
  and the datapath does not dial into a guest. Submit refuses
  `network.ports` on a gated sandbox (`ingress into a gated sandbox is not
  supported yet`). Wire the guest to a TAP to serve ports.
- The edge-boot `boot` gate uses the same policy-withheld marker the vsock
  listeners check, so a provisional sandbox has no network until it is
  ratified.
- `legion net doctor` and the UI report the sandbox like any gated
  allocation. `capture_ready` means both host sockets are bound, and an
  unbound one is named: `egress proxy is not bound on the guest's vsock
  socket <path>`.
- Every gated guest uses vsock CID 3. Both VMMs implement vsock over Unix
  sockets in user space, so a CID is private to its guest, and the datapath
  attributes a connection by the socket it arrived on, never by anything the
  guest says.

**Memory checkpoints.** A gated sandbox suspends, resumes, snapshots, and
forks with memory. The host socket path is derived from the allocation, and
every restore moves the guest onto the restoring allocation's own socket
(Firecracker's `vsock_override`, needing 1.16.1 or later, or
cloud-hypervisor's restored `config.json`). The new allocation's listeners
are bound before the restore, so a restored or forked guest's traffic is
attributed to the new allocation and judged by the policy in force at
restore. A checkpoint closes the guest's open connections: the guest agent
closes the relays it holds when it sees the restore, so clients get a prompt
close rather than a hang. See
[suspend-and-resume.md](suspend-and-resume.md#gated-sandboxes).

Current status: the host side (VMM configuration, listeners, status, the
restore-time socket move) and the agent are implemented and unit-tested over
Unix sockets with the VMMs' naming, and against fake VMM APIs. This
repository's CI has no KVM, so it does not run an end-to-end boot or restore
with a real VMM. `auto` resolves to `host` for sandboxes, as it does for
every runtime.
If the guest binary cannot run on the image (the glibc fallback on a musl
image), init reports it on the console and the guest has no network. This
fails closed, and `net doctor` shows the proxy never connecting. UDP inside
the guest and transparent
capture are not implemented.

Requirements and limits:

- Linux only. macOS has no network namespaces, so `gated` is rejected at
  start there with that reason.
- The worker needs `CAP_SYS_ADMIN`. It already does, for `exec` hardening's
  mount namespace and for running the OCI runtime.
- IPv4 loopback only inside the namespace.
- A gated `container` or `exec` task that declares no `connect.upstreams`
  has no resolver in its namespace. Proxy-aware tools work, because the
  proxy resolves the names they send it, but a tool that resolves a name
  itself fails. (A gated `sandbox` always gets its resolver.)
- No raw UDP and no ICMP.
- No inbound traffic except through the datapath's ingress listeners (below).

## The host guard

The host guard is a fixed set of destinations no allocation may reach
through the datapath: its own node, the cloud's link-local services, and
other allocations' listeners. It sits **below** policy. A job's rules and
stored policy objects can't widen it; only the node's operator can, in the
worker config.

| Class | Addresses | Why |
| --- | --- | --- |
| loopback | `127.0.0.0/8`, `::1` | the node's own services: Legion's ports, other allocations' upstreams and VIPs |
| unspecified | `0.0.0.0/8`, `::` | the same services by another name |
| this node | every address on the node's interfaces, re-read every heartbeat | the same services by the node's IP |
| link-local | `169.254.0.0/16`, `fe80::/10` | AWS, GCP, Azure, OCI, and DigitalOcean metadata, ECS task metadata (`169.254.170.2`), the VPC resolver |
| cloud metadata | `fd00:ec2::254`, `100.100.100.200` | AWS IPv6 metadata, Alibaba Cloud metadata |
| `hostGuard.deny` | operator CIDRs | anything else the operator wants treated like metadata |

IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`) are judged as the IPv4
address they map.

**Where it applies.** Every connection the datapath dials to a destination
the allocation named:

- the gated egress proxy, on all three paths (HTTP `CONNECT`, absolute-URI
  HTTP, SOCKS5);
- external upstreams (`to = "tcp://…"`, `tls://`, `http://`, `https://`), in
  L4 and HTTP mode;
- `proxy` handler targets, on egress and ingress. A handler that points at
  a node-local proxy (`target = "127.0.0.1:15001"`) needs a
  `hostGuard.allow` entry for that port; until then its health reports
  `host-guard: 127.0.0.1 is loopback; allocations may not reach it`.

Service upstreams are not judged: their endpoints come from the service
catalog, which Legion fills, not from the allocation. Neither are `authz`
handler calls, which the worker makes itself with request facts, never task
bytes, and whose answer is a verdict the task never sees.

**Resolve once, judge, dial what was judged.** A hostname is resolved once,
every address it resolves to is judged, and the dial goes only to an address
that passed. Nothing resolves the name again between the verdict and the
connect, so `localhost`, `2130706433`, `0177.0.0.1`, a public name that
resolves to `169.254.169.254`, and a name that rebinds between two lookups
are all refused. A name that resolves to a mix of addresses dials only the
ones that passed. CIDR rules in policy are matched against the same resolved
addresses, so a hostname can't reach a range a CIDR rule denies.

**Order of checks** on every gated listener: the edge-boot `boot` gate, then
the host guard, then `policy-pending`, then the egress chain. A host-guard
denial is final; no policy can turn it into an allow.

**What a task sees** is the ordinary denial shape with source `host-guard`:

```console
$ curl -sS -x $HTTP_PROXY http://169.254.169.254/latest/meta-data/   # inside a gated task
curl: (56) CONNECT tunnel failed, response 403

HTTP/1.1 403 Forbidden
Legion-Denied-By: host-guard
Legion-Denied-Reason: 169.254.169.254 is link-local (cloud metadata); allocations may not reach it
```

An L4 upstream closes the connection instead. Either way the denial is
reported on the next heartbeat as a `net.denied` event with
`source = "host-guard"`, and `legion net policy test` explains it before the
chain:

```console
$ legion net policy test web --to http://instance-data.ec2.internal/
✗ deny host-guard: instance-data.ec2.internal resolves to 169.254.169.254 (link-local); allocations may not reach it. Hint: the host guard is set on the worker (hostGuard.allow), not in job policy
```

`net policy test` runs on the manager, so it judges the built-in classes
(and resolves names from the manager). A node's own addresses and its
`hostGuard` entries are judged on the worker; a `net.denied` event from the
node is the authority. `legion net doctor` lists the guard as an
informational step for every allocation whose egress proxy is up.

**Operator overrides**, in the worker-type config, for the rare host service
an allocation should reach on purpose:

```nickel
hostGuard = {
  # Opened on purpose: the node exporter, for a metrics-scraping job.
  allow = ["10.0.1.7:9100"],
  # Extra destinations to treat like metadata.
  deny = ["192.0.2.10/32"],
},
```

`allow` takes `ip:port` or `cidr:port` (`[fd00::7]:9100` for IPv6) and
nothing broader, so an override opens one service and not the node. The
built-in classes can't be removed, only punched through one port at a time.
An invalid entry fails manager startup with the entry named. A bare address
in `allow` is refused for the same reason.

**Limits.**

- The guard is in the datapath. A `host`-mode task dials the network
  directly, so nothing stops it reaching the node or the metadata service;
  use `gated`, and the defense in depth under "Cloud metadata" below.
- A root `exec` task in `gated` mode can `setns` back into the host
  namespace.
- The guard judges destinations, not the node's own outbound traffic:
  artifact fetches, `authz` webhooks, and the worker's own calls to the
  manager are not judged.

### Cloud metadata

The cloud instance metadata service (`169.254.169.254`, `fd00:ec2::254`,
`100.100.100.200`) serves the instance's role credentials to anyone who can
reach it. The host guard refuses it for every connection the datapath
dials, so a gated task cannot reach it through Legion. A `host`-mode task
can, because its traffic does not pass through Legion.

As defense in depth on AWS, require IMDSv2 and set the hop limit to 1, so a
token request from inside a container's network namespace never gets an
answer even if something routes it:

```console
$ aws ec2 modify-instance-metadata-options --instance-id i-0123456789abcdef0 \
    --http-tokens required --http-put-response-hop-limit 1
```

GCP and Azure require a `Metadata-Flavor: Google` or `Metadata: true`
header, which a browser-style SSRF can't set but a task can. Grant each
node's instance role only what the worker itself needs.

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
- **Egress is cooperative in `host` mode, and `gated` mode is not yet a full
  boundary.** In `host` mode, policy applies to traffic that goes through an
  upstream and to names resolved through the allocation's resolver. A
  process that dials a raw address bypasses it. `mode = "gated"` (Linux,
  `container` and `exec`) closes that gap, because the namespace has no
  route except Legion's listeners, and every destination those listeners
  dial is judged by the [host guard](#the-host-guard) before policy. A root
  `exec` task can still leave the namespace, and a host-mode task can still
  dial the node and the metadata service directly, so `host` mode is
  cooperative. On macOS, egress stays cooperative, and
  there are no VIPs, per-allocation resolvers, or gated namespaces. A gated
  `sandbox` is captured over vsock. That path is implemented and
  unit-tested but not exercised on a real VMM in CI. `auto` resolves to
  `host` for every runtime, so set `gated` explicitly.
- **Ingress is enforced everywhere it applies.** The datapath owns the
  advertised port and the task binds a private twin, on every runtime and
  platform. TCP only. The task sees the datapath as the peer, not the
  original client.
- **TLS**: ingress chains terminate `server`/`mutual` TLS with your files, a
  certificate held in KV (swapped live on renewal), or the mesh certificate.
  Legion stores and serves certificates but does not issue public ones: a
  renewal job (or any ACME client) writes them to KV. The mesh is node-to-node mTLS with attributed
  workload identity, never per-workload certificates. There is no kTLS fast
  path (a terminated connection is copied in user space), no revocation
  beyond the 24-hour lifetime, and an ingress `proxy` hop cannot reach a
  gated task's private port.
- **No HTTP/2 or HTTP/3** (`protocol = "h2"` is rejected with that reason),
  no UDP, no WASM handlers, and no TLS interception. Legion will not add TLS
  interception. Use external upstreams when policy needs to see the body.
- **Legion has no job namespaces.** Every job is in `default`, so
  `selector.namespaces` either matches every job or none.
